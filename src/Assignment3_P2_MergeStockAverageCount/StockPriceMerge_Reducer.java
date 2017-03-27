/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P2_MergeStockAverageCount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class StockPriceMerge_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
            Context context
    ) throws IOException, InterruptedException {
        
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
