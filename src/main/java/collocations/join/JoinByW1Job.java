package collocations.join;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.fs.Path;

public class JoinByW1Job extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JoinByW1Job(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            if (args.length != 2) {
                System.err.println("Usage: JoinByW1Job <inputCounts> <output>");
                return 1;
            }
            Configuration conf = getConf();
            conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
            conf.set("mapreduce.output.textoutputformat.separator", "\t");
            Job job = Job.getInstance(conf);
            job.setJobName("collocations-join-w1");
            job.setJarByClass(JoinByW1Job.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(W1Mapper.class);
            job.setMapOutputKeyClass(W1Key.class);
            job.setMapOutputValueClass(W1Value.class);

            job.setPartitionerClass(W1Partitioner.class);
            job.setGroupingComparatorClass(W1GroupingComparator.class);
            job.setReducerClass(W1Reducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            System.err.println("JoinByW1Job failed");
            e.printStackTrace(System.err);
            throw e;
        }
    }

    public static class W1Key implements WritableComparable<W1Key> {
        private final Text lang = new Text();
        private final Text decade = new Text();
        private final Text w1 = new Text();
        private final Text tag = new Text(); // C1 first, then C12

        public W1Key() {}

        public W1Key(String lang, String decade, String w1, String tag) {
            this.lang.set(lang);
            this.decade.set(decade);
            this.w1.set(w1);
            this.tag.set(tag);
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            lang.write(out);
            decade.write(out);
            w1.write(out);
            tag.write(out);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            lang.readFields(in);
            decade.readFields(in);
            w1.readFields(in);
            tag.readFields(in);
        }

        @Override
        public int compareTo(W1Key other) {
            int c = lang.compareTo(other.lang);
            if (c != 0) return c;
            c = decade.compareTo(other.decade);
            if (c != 0) return c;
            c = w1.compareTo(other.w1);
            if (c != 0) return c;
            return tag.compareTo(other.tag);
        }

        public String getLang() { return lang.toString(); }
        public String getDecade() { return decade.toString(); }
        public String getW1() { return w1.toString(); }
    }

    public static class W1Value implements org.apache.hadoop.io.Writable {
        private boolean isC1;
        private final Text w2 = new Text();
        private final Text count = new Text();

        public W1Value() {}

        public static W1Value c1(String count) {
            W1Value v = new W1Value();
            v.isC1 = true;
            v.count.set(count);
            return v;
        }

        public static W1Value c12(String w2, String count) {
            W1Value v = new W1Value();
            v.isC1 = false;
            v.w2.set(w2);
            v.count.set(count);
            return v;
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            out.writeBoolean(isC1);
            w2.write(out);
            count.write(out);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            isC1 = in.readBoolean();
            w2.readFields(in);
            count.readFields(in);
        }

        public boolean isC1() { return isC1; }
        public String getW2() { return w2.toString(); }
        public String getCount() { return count.toString(); }
    }

    public static class W1Mapper extends Mapper<Object, Text, W1Key, W1Value> {
        @Override
        protected void map(Object offset, Text line, Context context) throws IOException, InterruptedException {
            String[] parts = line.toString().split("\\t");
            if (parts.length < 4) {
                return;
            }
            String lang = parts[0];
            String decade = parts[1];
            String tag = parts[2];
            if ("C1".equals(tag)) {
                if (parts.length < 5) return;
                String w1 = parts[3];
                String count = parts[parts.length - 1];
                context.write(new W1Key(lang, decade, w1, "0"), W1Value.c1(count));
            } else if ("C12".equals(tag)) {
                if (parts.length < 6) return;
                String w1 = parts[3];
                String w2 = parts[4];
                String count = parts[parts.length - 1];
                context.write(new W1Key(lang, decade, w1, "1"), W1Value.c12(w2, count));
            }
        }
    }

    public static class W1Partitioner extends Partitioner<W1Key, W1Value> {
        @Override
        public int getPartition(W1Key key, W1Value value, int numPartitions) {
            int hash = 31;
            hash = hash * 17 + key.getLang().hashCode();
            hash = hash * 17 + key.getDecade().hashCode();
            hash = hash * 17 + key.getW1().hashCode();
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class W1GroupingComparator extends WritableComparator {
        protected W1GroupingComparator() {
            super(W1Key.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            W1Key k1 = (W1Key) a;
            W1Key k2 = (W1Key) b;
            int c = k1.getLang().compareTo(k2.getLang());
            if (c != 0) return c;
            c = k1.getDecade().compareTo(k2.getDecade());
            if (c != 0) return c;
            return k1.getW1().compareTo(k2.getW1());
        }
    }

    public static class W1Reducer extends Reducer<W1Key, W1Value, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void reduce(W1Key key, Iterable<W1Value> values, Context context)
                throws IOException, InterruptedException {
            long c1 = -1L;
            for (W1Value v : values) {
                if (v.isC1()) {
                    try {
                        c1 = Long.parseLong(v.getCount());
                    } catch (NumberFormatException ignore) {
                        continue;
                    }
                    continue;
                }
                if (c1 < 0) {
                    continue;
                }
                String w2 = v.getW2();
                long c12;
                try {
                    c12 = Long.parseLong(v.getCount());
                } catch (NumberFormatException ignore) {
                    continue;
                }
                outKey.set(buildKey(key.getLang(), key.getDecade(), key.getW1(), w2));
                outVal.set(c12 + "\t" + c1);
                context.write(outKey, outVal);
            }
        }

        private String buildKey(String lang, String decade, String w1, String w2) {
            return lang + "\t" + decade + "\t" + w1 + "\t" + w2;
        }
    }
}
