/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment4_P4_MemoryConscious;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 *
 * @author Chintan
 */
public class MovingRatingMemConscious_Combiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {
    private SortedMapWritable result = new SortedMapWritable();

    public void reduce(IntWritable key, Iterable<SortedMapWritable> values,  Context context) throws IOException, InterruptedException {

        // loop through each hashmap for this movie id
        for (SortedMapWritable val : values) {
            // inside each hashmap, loop for every entry
            for(Map.Entry<WritableComparable, Writable> entry : val.entrySet()) {
                // check if current entry's key is already present in new hashmap
                if (result.containsKey(entry.getKey())) {
                    //if yes, extract current value from result hashmap for this key
                    LongWritable existingValue = (LongWritable) result.get(entry.getKey()) ;

                    // increment existing value by 1
                    existingValue.set(existingValue.get()+1);

                    // update result hashmap with new value
                    result.put(entry.getKey(),  existingValue);
                } else {
                    //if not, create new entry with init value 1
                    result.put(entry.getKey(), new LongWritable(1));
                }
            }

            val.clear();
        }

        context.write(key, result);
    }
}


// /LongWritable originalValue = (LongWritable) ( result.get(entry.getKey()) ) + (LongWritable) entry.getValue();
//result.put(entry.getKey(), ( result.get(entry.getKey()) + new LongWritable(1) );
