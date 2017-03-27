/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment2_P4_MovieRatingCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 * @author Chintan
 */
public class MovieRating_Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private IntWritable movieID;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("Content on line " + key.toString() + " :- " + value.toString() );

        String[] movieInfo = value.toString().split("::");
        //System.out.println("String is split now ");

        if(!movieInfo[1].trim().toLowerCase().equals("movieid")) {
            movieID = new IntWritable(Integer.parseInt(movieInfo[1]));
            //System.out.println("MovieId extracted " + movieID.toString());

            context.write(movieID, one);
        }
    }
}
