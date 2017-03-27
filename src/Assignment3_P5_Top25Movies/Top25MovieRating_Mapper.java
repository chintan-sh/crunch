/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P5_Top25Movies;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top25MovieRating_Mapper extends Mapper<Object, Text, IntWritable, FloatWritable> {

    private IntWritable movieID;
    private FloatWritable rating;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] movieInfo = value.toString().split("::");

        if(!movieInfo[1].trim().toLowerCase().equals("movieid")) {
            movieID = new IntWritable(Integer.parseInt(movieInfo[1]));
            rating = new FloatWritable(Float.parseFloat(movieInfo[2]));
            //System.out.println("MovieId extracted " + movieID.toString());

            context.write(movieID, rating);
        }
    }
}
