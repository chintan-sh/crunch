/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P3_PartitionPattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Partition_IPAddress_By_MonthDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IP Address By Date");
        job.setJarByClass(Partition_IPAddress_By_MonthDriver.class);
        job.setMapperClass(Partition_IPAddress_By_Month_Mapper.class);
        //job.setCombinerClass(Partition_IPAddress_By_Month_Reducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // partitioner class inclusion
        job.setPartitionerClass(Partition_IPAddress_By_Month_Partitioner.class);

        // set num of reduce tasks based on partition we need (here we need 12 cos total no.of months in a year)
        job.setNumReduceTasks(12);
        job.setReducerClass(Partition_IPAddress_By_Month_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
