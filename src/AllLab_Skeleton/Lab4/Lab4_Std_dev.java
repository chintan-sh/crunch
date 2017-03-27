/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AllLab_Skeleton.Lab4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import static java.util.Collections.list;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pooja
 */
public class Lab4_Std_dev {

    /**
     * @param args the command line arguments
     */
    public static class Map extends Mapper<Object, Text, Text, DoubleWritable>{
        
        public void map(Object key, Text value, Context context){
            try{
                String row[] = value.toString().split(",");
                String stock = row[1];
                String price = row[4];
                double pri = Double.parseDouble(price);
                
                context.write(new Text(stock), new DoubleWritable(pri));
                
            }catch(Exception e){
                
            }
        }       
    }
    
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, MedianSDCustomWritable>{
        
        private MedianSDCustomWritable result = new MedianSDCustomWritable();
        private ArrayList<Double> list = new ArrayList<Double>();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            
            double sum = 0;
            double count = 0;
            list.clear();
            result.setStandardDeviation(0);
            
            for(DoubleWritable val : values){
                list.add(val.get());
                sum += val.get();
                count++;            
            }
            
            Collections.sort(list);
            
            if(count % 2 == 0){
                result.setMedian((list.get((int)count/2) + list.get((int)count/2 - 1))/2);
               
            }else{
                result.setMedian(list.get((int) count/2));
            }
            
            double mean = sum/count;
            double sumOfSquares = 0;
            
            for(double val : list){
                sumOfSquares += (val - mean)*(val - mean);
            }
            result.setStandardDeviation((double) Math.sqrt(sumOfSquares / (count - 1)));
            context.write(key, result);
        }    
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "medianstd");
        job.setJarByClass(Lab4_Std_dev.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianSDCustomWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
    
}
