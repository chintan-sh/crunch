/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AllLab_Skeleton.Lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class WordCount_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static int count = 0;

    public void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

        for (IntWritable val : values) {
            count += val.get();
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Count : "), new IntWritable(count));
    }
}

// arr[1] = stock symbol
// arr[4] = stock price high