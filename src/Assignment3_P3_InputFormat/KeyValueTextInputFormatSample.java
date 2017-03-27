package Assignment3_P3_InputFormat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author chintan
 */
public class KeyValueTextInputFormatSample {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "key value input format sample");
        job.setJarByClass(KeyValueTextInputFormatSample.class);
        job.setMapperClass(KeyValueTextInputFormatSample_Mapper.class);
        job.setCombinerClass(KeyValueTextInputFormatSample_Reducer.class);
        job.setReducerClass(KeyValueTextInputFormatSample_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
