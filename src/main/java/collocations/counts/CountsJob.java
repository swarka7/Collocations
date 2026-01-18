package collocations.counts;

import collocations.core.ConfigUtil;
import collocations.core.DebugUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Counts bigrams, w1 marginals, w2 marginals, and N for each lang+decade.
 */
public class CountsJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println("BUILD_FINGERPRINT=2025-12-20T21:38Z");
        try {
            int res = ToolRunner.run(new Configuration(), new CountsJob(), args);
            System.exit(res);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.err.flush();
            throw t;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            if (args.length < 4) {
                System.err.println("Usage: CountsJob <input> <output> <lang> <stopwordsResourceName> [--useCombiner true|false]");
                return 1;
            }
            Configuration conf = getConf();
            ConfigUtil.setCommonOutputs(conf);
            conf.set("colloc.lang", ConfigUtil.sanitize("colloc.lang", args[2]));
            conf.set("colloc.stopwords", ConfigUtil.sanitize("colloc.stopwords", args[3]));
            System.err.println("JOB_COUNTS_START lang=" + args[2] + " stopwords=" + args[3]);
            boolean useCombiner = true;
            if (args.length >= 5) {
                String combArg = args[4];
                String[] flag = combArg.split("=");
                if (flag.length == 2 && "--useCombiner".equals(flag[0])) {
                    useCombiner = Boolean.parseBoolean(flag[1]);
                } else if ("true".equalsIgnoreCase(combArg) || "false".equalsIgnoreCase(combArg)) {
                    useCombiner = Boolean.parseBoolean(combArg);
                } else if (combArg.startsWith("--useCombiner")) {
                    useCombiner = true;
                }
            }

            Job job = Job.getInstance(conf);
            job.setJobName(ConfigUtil.sanitize("jobName", "collocations-counts-" + conf.get("colloc.lang", "lang")));
            job.setJarByClass(CountsJob.class);
            // So the single reducer does not blow up in shuffle, spread across multiple reducers.
            int reducers = conf.getInt("colloc.reducers", 8);
            job.setNumReduceTasks(reducers);
            // Tighter shuffle memory limits to reduce OOM risk on large map outputs.
            job.getConfiguration().setFloat("mapreduce.reduce.shuffle.memory.limit.percent", 0.10f);
            job.getConfiguration().setFloat("mapreduce.reduce.shuffle.input.buffer.percent", 0.20f);
            job.getConfiguration().setFloat("mapreduce.reduce.input.buffer.percent", 0.0f);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(CountsMapper.class);
            if (useCombiner) {
                job.setCombinerClass(CountsReducer.class);
            }
            job.setReducerClass(CountsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            MultipleOutputs.addNamedOutput(job, "N", TextOutputFormat.class, Text.class, LongWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            ConfigUtil.validateNoControlChars("mapreduce.output.textoutputformat.separator", conf.get("mapreduce.output.textoutputformat.separator"));
            ConfigUtil.validateNoControlChars("colloc.lang", conf.get("colloc.lang"));
            ConfigUtil.validateNoControlChars("colloc.stopwords", conf.get("colloc.stopwords"));
            ConfigUtil.validateNoControlChars("jobName", job.getJobName());

            boolean success = job.waitForCompletion(true);
            if (success) {
                writeCounters(job, new Path(args[1]));
            }
            return success ? 0 : 1;
        } catch (Exception e) {
            System.err.println("CountsJob failed");
            e.printStackTrace(System.err);
            throw e;
        }
    }

    private void writeCounters(Job job, Path outDir) {
        try {
            Counters counters = job.getCounters();
            Counter mapOutRecs = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
            Counter mapOutBytes = counters.findCounter(TaskCounter.MAP_OUTPUT_BYTES);
            Counter redInBytes = mapOutBytes;
            Counter redInRecs = counters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS);

            FileSystem fs = outDir.getFileSystem(job.getConfiguration());
            Path report = new Path(outDir, "counters-report.txt");
            try (FSDataOutputStream out = fs.create(report, true)) {
                out.writeBytes("map_output_records\t" + (mapOutRecs == null ? 0 : mapOutRecs.getValue()) + "\n");
                out.writeBytes("map_output_bytes\t" + (mapOutBytes == null ? 0 : mapOutBytes.getValue()) + "\n");
                out.writeBytes("reduce_input_records\t" + (redInRecs == null ? 0 : redInRecs.getValue()) + "\n");
                out.writeBytes("reduce_input_bytes\t" + (redInBytes == null ? 0 : redInBytes.getValue()) + "\n");
            }
        } catch (Exception e) {
            System.err.println("Failed to write counters report");
            e.printStackTrace(System.err);
        }
    }

    public static class CountsMapper extends Mapper<WritableComparable<?>, Writable, Text, LongWritable> {
        private static final String TAG_C12 = "C12";
        private static final String TAG_C1 = "C1";
        private static final String TAG_C2 = "C2";
        private static final String TAG_N = "N";

        private final Set<String> stopwords = new HashSet<>();
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();
        private String lang = "en";
        private boolean printedDebug = false;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            lang = conf.get("colloc.lang", "en");
            String stopwordsResource = conf.get("colloc.stopwords", "eng-stopwords.txt");
            try {
                loadStopwords(stopwordsResource);
            } catch (Throwable t) {
                System.err.println("Failed to load stopwords: " + stopwordsResource);
                t.printStackTrace(System.err);
                System.err.flush();
                throw new IOException("Failed to load stopwords: " + stopwordsResource, t);
            }
        }

        private void loadStopwords(String resourceName) throws IOException {
            ClassLoader cl = getClass().getClassLoader();
            InputStream in = cl.getResourceAsStream(resourceName);
            if (in == null) {
                in = cl.getResourceAsStream("/" + resourceName);
            }
            if (in == null) {
                java.io.File f = new java.io.File(resourceName);
                if (f.exists()) {
                    in = new java.io.FileInputStream(f);
                }
            }
            if (in == null) {
                throw new IllegalArgumentException("Stopwords not found. classpath? filesystem? arg=" + resourceName);
            }
            try (InputStream stream = in;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                        continue;
                    }
                    stopwords.add(trimmed);
                }
            }
        }

        @Override
        protected void map(WritableComparable<?> key, Writable value, Context context)
                throws IOException, InterruptedException {
            if (!printedDebug) {
                printedDebug = true;
                System.err.println("DEBUG key class: " + key.getClass().getName());
                System.err.println("DEBUG value class: " + value.getClass().getName());
                System.err.println("DEBUG key sample: " + truncate(key.toString(), 500));
                System.err.println("DEBUG value sample: " + truncate(value.toString(), 500));
            }

            ParsedRecord parsed = parseRecord(key.toString(), value.toString());
            if (parsed == null) {
                return;
            }
            String w1 = parsed.w1;
            String w2 = parsed.w2;
            if (stopwords.contains(w1) || stopwords.contains(w2)) {
                return;
            }
            int year = parsed.year;
            long count = parsed.count;

            long decade = (year / 10) * 10L;
            outVal.set(count);

            outKey.set(buildKey(lang, decade, TAG_C12, w1, w2));
            context.write(outKey, outVal);

            outKey.set(buildKey(lang, decade, TAG_C1, w1, null));
            context.write(outKey, outVal);

            outKey.set(buildKey(lang, decade, TAG_C2, w2, null));
            context.write(outKey, outVal);

            outKey.set(buildKey(lang, decade, TAG_N, null, null));
            context.write(outKey, outVal);
        }

        private String buildKey(String lang, long decade, String tag, String p1, String p2) {
            StringBuilder sb = new StringBuilder();
            sb.append(lang).append('\t').append(decade).append('\t').append(tag);
            if (p1 != null) {
                sb.append('\t').append(p1);
            }
            if (p2 != null) {
                sb.append('\t').append(p2);
            }
            return sb.toString();
        }

        private String truncate(String s, int max) {
            if (s == null) {
                return "";
            }
            return s.length() <= max ? s : s.substring(0, max);
        }

        private static class ParsedRecord {
            final String w1;
            final String w2;
            final int year;
            final long count;

            ParsedRecord(String w1, String w2, int year, long count) {
                this.w1 = w1;
                this.w2 = w2;
                this.year = year;
                this.count = count;
            }
        }

        /**
         * Robustly parse w1, w2, year, count from arbitrary key/value tokens.
         * We collect word tokens and numeric tokens separately from both key and value.
         * - w1/w2 = first two word tokens
         * - year  = first numeric in [1400, 2100)
         * - count = next numeric > 0
         */
        private ParsedRecord parseRecord(String keyStr, String valStr) {
            java.util.List<String> words = new java.util.ArrayList<>();
            java.util.List<Long> nums = new java.util.ArrayList<>();
            collectTokens(keyStr, words, nums);
            collectTokens(valStr, words, nums);

            if (words.size() < 2 || nums.size() < 2) {
                return null;
            }

            int year = -1;
            long count = -1L;
            for (Long n : nums) {
                if (year < 0 && n >= 1400 && n < 2100) {
                    year = n.intValue();
                    continue;
                }
                if (year >= 0 && count < 0 && n > 0) {
                    count = n;
                    break;
                }
            }
            if (year < 0 || count < 0) {
                return null;
            }
            return new ParsedRecord(words.get(0), words.get(1), year, count);
        }

        private void collectTokens(String text, java.util.List<String> words, java.util.List<Long> nums) {
            if (text == null) {
                return;
            }
            String[] parts = text.trim().split("\\s+");
            for (String p : parts) {
                if (p.isEmpty()) {
                    continue;
                }
                try {
                    long n = Long.parseLong(p);
                    nums.add(n);
                } catch (NumberFormatException ignore) {
                    words.add(p);
                }
            }
        }
    }

    public static class CountsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable outValue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable val : values) {
                sum += val.get();
            }
            outValue.set(sum);
            context.write(key, outValue);
        }
    }
}
