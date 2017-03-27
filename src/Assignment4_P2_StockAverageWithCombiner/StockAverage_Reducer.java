/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment4_P2_StockAverageWithCombiner;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class StockAverage_Reducer extends Reducer<Text, StockAverage_CompositeValueWritable, Text, StockAverage_CompositeValueWritable> {
    private StockAverage_CompositeValueWritable result = new StockAverage_CompositeValueWritable();

    public void reduce(Text key, Iterable<StockAverage_CompositeValueWritable> values,  Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        for (StockAverage_CompositeValueWritable val : values) {
            sum += val.getCount() * Float.parseFloat(val.getAverage());
            count += val.getCount();
        }

        float average = sum/count;
        result.setCount(count);
        result.setAverage(String.valueOf(average));
        context.write(key, result);
    }
}
