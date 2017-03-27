/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment2_P2_StockExchangeCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class StockPrice_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        float count = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count = count + 1;
        }

        float average = sum/count;
        result.set(average);
        context.write(key, result);
    }
}

// arr[1] = stock symbol
// arr[4] = stock price high