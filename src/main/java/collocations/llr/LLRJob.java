package collocations.llr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LLRJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LLRJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            if (args.length != 3) {
                System.err.println("Usage: LLRJob <joinedInput> <countsDirForN> <output>");
                return 1;
            }
            Configuration conf = getConf();
            conf.set("mapreduce.output.textoutputformat.separator", "\t");
            Job job = Job.getInstance(conf);
            job.setJobName("collocations-llr");
            job.setJarByClass(LLRJob.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(JobComputeLLRMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(0);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            // Pass counts dir for N loading; mapper will read N records directly.
            job.getConfiguration().set("colloc.counts.dir", args[1]);

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            System.err.println("LLRJob failed");
            e.printStackTrace(System.err);
            throw e;
        }
    }
}
