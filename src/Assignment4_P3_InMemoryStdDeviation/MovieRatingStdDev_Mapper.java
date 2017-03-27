/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment4_P3_InMemoryStdDeviation;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class MovieRatingStdDev_Mapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
    private FloatWritable rating;
    private IntWritable movieID;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] movieInfo = value.toString().split("::");

        if(!movieInfo[1].trim().toLowerCase().equals("movieid")) {
            movieID = new IntWritable(Integer.parseInt(movieInfo[1]));
            rating = new FloatWritable(Float.parseFloat(movieInfo[2]));

            context.write(movieID, rating);
        }
    }
}
