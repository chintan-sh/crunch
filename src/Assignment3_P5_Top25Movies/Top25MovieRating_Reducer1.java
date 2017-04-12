/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P5_Top25Movies;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 *
 * @author Chintan
 */
public class Top25MovieRating_Reducer1 extends Reducer<FloatWritable, IntWritable, FloatWritable, IntWritable> {
    private HashMap<FloatWritable, IntWritable> countMap = new HashMap<>();
    private static int counter = 0;

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        for (FloatWritable val : values) {
                // puts the number of occurrences of this word into the map.
            if (counter <= 25) {
                 countMap.put(val, key);
                 counter = counter+1;
            }else{
                System.exit(1);
            }
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        for (FloatWritable key: countMap.keySet()) {
//            if (counter++ == 25) {
//                System.exit(1);
//            }
//
//            context.write(key, countMap.get(key));
//        }
//    }
}
