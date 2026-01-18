package collocations.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopKJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopKJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            if (args.length != 2) {
                System.err.println("Usage: TopKJob <llrInput> <output>");
                return 1;
            }
            Configuration conf = getConf();
            conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
            Job job = Job.getInstance(conf);
            job.setJobName("collocations-top100");
            job.setJarByClass(TopKJob.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(JobTop100Mapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(JobTop100Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            System.err.println("TopKJob failed");
            e.printStackTrace(System.err);
            throw e;
        }
    }
}
