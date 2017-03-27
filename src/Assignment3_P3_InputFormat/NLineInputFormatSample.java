package Assignment3_P3_InputFormat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author chintan
 */
public class NLineInputFormatSample extends Configured implements Tool {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        int ex = ToolRunner.run(new NLineInputFormatSample(), args);
        System.exit(ex);
    }
    
    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 1000);
        
        Job job = Job.getInstance(conf,"nline Input Format Sample");
        job.setJarByClass(NLineInputFormatSample.class);

        job.setMapperClass(NLineInputFormatSample_Mapper.class);
        job.setReducerClass(NLineInputFormatSample_Reducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0:1;
    }
    
}
