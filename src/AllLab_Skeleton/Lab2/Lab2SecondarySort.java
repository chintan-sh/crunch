/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AllLab_Skeleton.Lab2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pooja
 */
public class Lab2SecondarySort {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "SecondarySort");
            job.setJarByClass(Lab2SecondarySort.class);
           
            job.setMapperClass(Lab2Mapper.class);
            job.setMapOutputKeyClass(CompositeKeyWritable.class);
            job.setMapOutputValueClass(NullWritable.class);
            
            job.setPartitionerClass(Lab2Partitioner.class);
            job.setGroupingComparatorClass(Lab2GroupComparator.class);
            
            job.setReducerClass(Lab2Reducer.class);
            job.setOutputKeyClass(CompositeKeyWritable.class);
            job.setOutputValueClass(NullWritable.class);
            
            job.setNumReduceTasks(8);
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
            
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            System.out.println("Erorr Message"+ ex.getMessage());
        }
    }
    
}
