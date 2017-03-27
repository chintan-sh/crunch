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

/**
 *
 * @author Chintan
 */
public class Top25MovieRating_Reducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count = count + 1;
        }

        float avg = sum / count;
        result.set(avg);
        context.write(key, result);
    }
}
