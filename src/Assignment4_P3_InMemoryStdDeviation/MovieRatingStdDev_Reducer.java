/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment4_P3_InMemoryStdDeviation;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 *
 * @author Chintan
 */
public class MovieRatingStdDev_Reducer extends Reducer<IntWritable, FloatWritable, IntWritable, Text> {
    private Text result;
    private ArrayList<Float> list = new ArrayList<>();

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        int running_sum = 0;
        int running_count = 0;
        float median = 0;

        for (FloatWritable val : values) {
            list.add((float)val.get());
            running_sum += val.get();
            running_count++;
        }

        Collections.sort(list);

        // calculating median
        if(list.size() % 2 == 0){
            median = (list.get((running_count/2)-1) +  list.get((running_count/2)))/2;
        }else{
            median = list.get((running_count/2));
        }

        // calculating mean
        float mean = running_sum/running_count;

        // calculating standard deviation
        float sumSquare = 0;
        float stdDev = 0;
        for(Float oneValue : list){
            sumSquare += (oneValue - mean) * (oneValue - mean);
        }

        // finally, std dev
        stdDev = (float) Math.sqrt( sumSquare / (running_count-1) );

        //.append(median)
        String outcome = new StringBuilder().append("\t").append(stdDev).append("\t").append(running_sum).append("\t").append(running_count).toString();
        result = new Text(outcome);
        context.write(key, result);
    }
}
