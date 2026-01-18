package collocations.join;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinByW2Job extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JoinByW2Job(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            if (args.length != 3) {
                System.err.println("Usage: JoinByW2Job <joinedByW1Input> <countsInputForC2> <output>");
                return 1;
            }
            Configuration conf = getConf();
            conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
            conf.set("mapreduce.output.textoutputformat.separator", "\t");
            Job job = Job.getInstance(conf);
            job.setJobName("collocations-join-w2");
            job.setJarByClass(JoinByW2Job.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BigramMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, C2Mapper.class);

            job.setMapOutputKeyClass(W2Key.class);
            job.setMapOutputValueClass(W2Value.class);
            job.setPartitionerClass(W2Partitioner.class);
            job.setGroupingComparatorClass(W2GroupingComparator.class);
            job.setReducerClass(W2Reducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            System.err.println("JoinByW2Job failed");
            e.printStackTrace(System.err);
            throw e;
        }
    }

    public static class W2Key implements WritableComparable<W2Key> {
        private final Text lang = new Text();
        private final Text decade = new Text();
        private final Text w2 = new Text();
        private final Text tag = new Text(); // 0=c2, 1=bigram

        public W2Key() {}

        public W2Key(String lang, String decade, String w2, String tag) {
            this.lang.set(lang);
            this.decade.set(decade);
            this.w2.set(w2);
            this.tag.set(tag);
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            lang.write(out);
            decade.write(out);
            w2.write(out);
            tag.write(out);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            lang.readFields(in);
            decade.readFields(in);
            w2.readFields(in);
            tag.readFields(in);
        }

        @Override
        public int compareTo(W2Key other) {
            int c = lang.compareTo(other.lang);
            if (c != 0) return c;
            c = decade.compareTo(other.decade);
            if (c != 0) return c;
            c = w2.compareTo(other.w2);
            if (c != 0) return c;
            return tag.compareTo(other.tag);
        }

        public String getLang() { return lang.toString(); }
        public String getDecade() { return decade.toString(); }
        public String getW2() { return w2.toString(); }
    }

    public static class W2Value implements org.apache.hadoop.io.Writable {
        private boolean isC2;
        private final Text w1 = new Text();
        private final Text c12 = new Text();
        private final Text c1 = new Text();
        private final Text c2 = new Text();

        public W2Value() {}

        public static W2Value c2(String count) {
            W2Value v = new W2Value();
            v.isC2 = true;
            v.c2.set(count);
            return v;
        }

        public static W2Value bigram(String w1, String c12, String c1) {
            W2Value v = new W2Value();
            v.isC2 = false;
            v.w1.set(w1);
            v.c12.set(c12);
            v.c1.set(c1);
            return v;
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            out.writeBoolean(isC2);
            w1.write(out);
            c12.write(out);
            c1.write(out);
            c2.write(out);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            isC2 = in.readBoolean();
            w1.readFields(in);
            c12.readFields(in);
            c1.readFields(in);
            c2.readFields(in);
        }

        public boolean isC2() { return isC2; }
        public String getW1() { return w1.toString(); }
        public String getC12() { return c12.toString(); }
        public String getC1() { return c1.toString(); }
        public String getC2() { return c2.toString(); }
    }

    public static class BigramMapper extends Mapper<Object, Text, W2Key, W2Value> {
        @Override
        protected void map(Object offset, Text line, Context context) throws IOException, InterruptedException {
            // line: lang \t decade \t w1 \t w2 \t c12 \t c1
            String[] parts = line.toString().split("\\t");
            if (parts.length < 6) {
                return;
            }
            String lang = parts[0];
            String decade = parts[1];
            String w1 = parts[2];
            String w2 = parts[3];
            String c12 = parts[4];
            String c1 = parts[5];
            context.write(new W2Key(lang, decade, w2, "1"), W2Value.bigram(w1, c12, c1));
        }
    }

    public static class C2Mapper extends Mapper<Object, Text, W2Key, W2Value> {
        @Override
        protected void map(Object offset, Text line, Context context) throws IOException, InterruptedException {
            // counts lines: lang \t decade \t TAG [\t term ...] \t count
            String[] parts = line.toString().split("\\t");
            if (parts.length < 4) {
                return;
            }
            if (!"C2".equals(parts[2])) {
                return;
            }
            String lang = parts[0];
            String decade = parts[1];
            String w2 = parts[3];
            String count = parts[parts.length - 1];
            context.write(new W2Key(lang, decade, w2, "0"), W2Value.c2(count));
        }
    }

    public static class W2Partitioner extends Partitioner<W2Key, W2Value> {
        @Override
        public int getPartition(W2Key key, W2Value value, int numPartitions) {
            int hash = 31;
            hash = hash * 17 + key.getLang().hashCode();
            hash = hash * 17 + key.getDecade().hashCode();
            hash = hash * 17 + key.getW2().hashCode();
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class W2GroupingComparator extends WritableComparator {
        protected W2GroupingComparator() {
            super(W2Key.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            W2Key k1 = (W2Key) a;
            W2Key k2 = (W2Key) b;
            int c = k1.getLang().compareTo(k2.getLang());
            if (c != 0) return c;
            c = k1.getDecade().compareTo(k2.getDecade());
            if (c != 0) return c;
            return k1.getW2().compareTo(k2.getW2());
        }
    }

    public static class W2Reducer extends Reducer<W2Key, W2Value, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void reduce(W2Key key, Iterable<W2Value> values, Context context)
                throws IOException, InterruptedException {
            long c2 = -1L;
            for (W2Value v : values) {
                if (v.isC2()) {
                    try {
                        c2 = Long.parseLong(v.getC2());
                    } catch (NumberFormatException ignore) {
                        continue;
                    }
                    continue;
                }
                if (c2 < 0) {
                    continue;
                }
                long c12, c1;
                try {
                    c12 = Long.parseLong(v.getC12());
                    c1 = Long.parseLong(v.getC1());
                } catch (NumberFormatException ignore) {
                    continue;
                }
                outKey.set(buildKey(key.getLang(), key.getDecade(), v.getW1(), key.getW2()));
                outVal.set(c12 + "\t" + c1 + "\t" + c2);
                context.write(outKey, outVal);
            }
        }

        private String buildKey(String lang, String decade, String w1, String w2) {
            return lang + "\t" + decade + "\t" + w1 + "\t" + w2;
        }
    }
}
