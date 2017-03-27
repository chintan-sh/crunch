/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment4_P4_MemoryConscious;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class MovieRatingMemConscious_Mapper extends Mapper<Object, Text, IntWritable, SortedMapWritable> {
    private IntWritable movieID;
    private FloatWritable rating;
    private LongWritable ONE = new LongWritable(1);
    private SortedMapWritable outSortMap = new SortedMapWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] movieInfo = value.toString().split("::");

        if(!movieInfo[1].trim().toLowerCase().equals("movieid")) {
            // extract movieID and rating
            movieID = new IntWritable(Integer.parseInt(movieInfo[1]));
            rating = new FloatWritable(Float.parseFloat(movieInfo[2]));

            // push rating in sorted hashmap
            outSortMap.put(rating, ONE);

            // send this fellow to combiner now, come on, do it bleeeeeedy
            context.write(movieID, outSortMap);
        }
    }
}
