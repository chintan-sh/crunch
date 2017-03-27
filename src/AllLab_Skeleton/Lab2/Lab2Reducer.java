/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AllLab_Skeleton.Lab2;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pooja
 */
public class Lab2Reducer extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {
    
    /**
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context) {
        for (NullWritable val : values){
            try {
                context.write(key, NullWritable.get());
            } catch (IOException | InterruptedException ex) {
                System.out.println("Error Message"+ ex.getMessage());
            }
        }
    }
    
    
}
