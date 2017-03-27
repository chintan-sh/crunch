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
import java.util.TreeMap;

/**
 *
 * @author Chintan
 */
public class MovieRatingMemConscious_Reducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, Text> {
    //private SortedMapWritable result= new SortedMapWritable();
    private Text result= new Text();
    private TreeMap<Float, Long> list;

    public void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
        int running_count = 0;
        long running_sum = 0;
        float median = 0;
        list = new TreeMap<>();

        // loop through all ratings in received hashmap for this movieID
        for (SortedMapWritable val : values) {

            // iterate through every entry consisting of movieRating, countOfRating for ex (4.5, 10) i.e 10 people rated it 4.5
            for (Map.Entry<WritableComparable, Writable> entry : val.entrySet()) {

                // extract movieRating for ex : 4.5
                FloatWritable number = (FloatWritable) entry.getKey();
                float movieRating = number.get();

                //extract countOfRating for ex : 10
                LongWritable counter = (LongWritable) entry.getValue();
                long count = counter.get();

                // calculate running sum by multiplying movieRating and countRating i.e (4.5 * 10 = 45)
                running_sum += (movieRating * count);

                // increment running count for getting average later i.e 10
                running_count += count;

                // make <count> entries for <movieRating> in new hashmap i.e  (4.5, 10)
                if(list.containsKey(movieRating)){
                    list.put(movieRating, list.get(movieRating) + count);
                }else{
                    list.put(movieRating, count);
                }

            }
        }

        System.out.println("Running count for movieID " + key + " is :- " + running_count);
        System.out.println("Rating List size for movieID " + key + " is :- " + list.size());



        // calculating mean
        float mean = running_sum/running_count;
        System.out.println("Mean for movieID " + key + " is :- " + mean);

        // calculating standard deviation
        float sumSquare = 0;
        float stdDev = 0;
        for (Map.Entry<Float, Long> entry : list.entrySet()) {
            sumSquare += (entry.getKey() - mean) * (entry.getKey() - mean) * (entry.getValue());
        }

        // finally, std dev
        stdDev = (float) Math.sqrt( sumSquare / ( running_count - 1 ) );
        System.out.println("Standard deviation for movieID " + key + " is :- " + stdDev);


        //.append(median)
        String outcome = new StringBuilder().append("\t").append(stdDev).append("\t").append(running_sum).append("\t").append(running_count).toString();
        result = new Text(outcome);
        context.write(key, result);
    }
}


// show me the median
//        if(list.size() % 2 == 0){
//                FloatWritable key1 = (FloatWritable) list.keySet().toArray()[(running_count/2)-1];
//                float middle = key1.get();
//
//                FloatWritable key2 = (FloatWritable) list.keySet().toArray()[(running_count/2)];
//                float nextMiddle = key2.get();
//
//                median = (middle +  nextMiddle) / 2 ;
//                }else{
//                FloatWritable key2 = (FloatWritable) list.keySet().toArray()[(running_count/2)];
//                float nextMiddle = key2.get();
//                median = nextMiddle;
//                }




//System.out.println("Median for movieID " + key + " is :- " + (running_count/2));

//list.keySet().toArray()[(running_count/2)-1];
//calculating median method 1
//        if(list.size() % 2 == 0){
//            FloatWritable key1 = (FloatWritable) list.keySet().toArray()[(running_count/2)-1];
//            float middle = key1.get();
//
//            FloatWritable key2 = (FloatWritable) list.keySet().toArray()[(running_count/2)];
//            float nextMiddle = key2.get();
//
//            median = (middle +  nextMiddle) / 2 ;
//        }else{
//            FloatWritable key2 = (FloatWritable) list.keySet().toArray()[(running_count/2)];
//            float nextMiddle = key2.get();
//
//            median = nextMiddle;
//        }



//calculating median method 2
//        if(list.size() % 2 == 0){
//                median = ( list.get(list.keySet().toArray()[(running_count/2)-1]) +  list.get(list.keySet().toArray()[(running_count/2)]) )/2;
//                }else{
//                median = list.get( list.keySet().toArray()[(running_count/2)] );
//                }



//                for (long i = 0; i < count; i++) {
//        if(i == 0){
//        System.out.println("Rating count for movieId " + key + " is :- " + outcome);
//        System.out.println("Total ratings of " + outcome + " received for movieID " + key + " is :- " + count);
//        }
//        list.put(outcome, 1);
//        }

// jai shree ram
//        FloatWritable standardDeviation = new FloatWritable(stdDev);
//        LongWritable finalSum = new LongWritable(running_sum);
//        result.put(standardDeviation, finalSum);


//        int faltucounter = 0;
//        faltucounter++;
//        System.out.println(faltucounter + " counter for movieID " + key + " has value :- " + entry.getKey());