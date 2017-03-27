/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P5_Top25Movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top25MovieRatingDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Movie Rating Count");
        job1.setJarByClass(Top25MovieRatingDriver.class);

        // the usual - get basic mapred ready
        job1.setMapperClass(Top25MovieRating_Mapper.class);
        job1.setCombinerClass(Top25MovieRating_Reducer.class);
        job1.setReducerClass(Top25MovieRating_Reducer.class);

        // this will basically out -> movieId, average rating
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean complete = job1.waitForCompletion(true);

        // here's where we sort
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Movie Rating Count");
        if (complete) {
            job2.setJarByClass(Top25MovieRatingDriver.class);

            // namesake fellow, take it and go types - mostly useless
            job2.setMapperClass(Top25MovieRating_Mapper1.class);
            job2.setMapOutputKeyClass(FloatWritable.class);
            job2.setMapOutputValueClass(IntWritable.class);

            // this is where we would ideally sort descendingly
            job2.setSortComparatorClass(Top25MovieRating_SortComparator.class);

            // o/p top 25, man
            job2.setNumReduceTasks(1);
            job2.setReducerClass(Top25MovieRating_Reducer1.class);
            job2.setOutputKeyClass(FloatWritable.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
