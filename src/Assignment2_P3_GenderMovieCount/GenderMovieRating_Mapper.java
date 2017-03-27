/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment2_P3_GenderMovieCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class GenderMovieRating_Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text gender;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("Content on line " + key.toString() + " :- " + value.toString() );

        String[] userInfo = value.toString().split("::");
        //System.out.println("String is split now ");

        gender = new Text(userInfo[1]);
        //System.out.println("MovieId extracted " + movieID.toString());

        context.write(gender, one);
    }
}
