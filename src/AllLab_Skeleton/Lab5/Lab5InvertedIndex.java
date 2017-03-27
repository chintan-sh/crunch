/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab5invertedindex;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author pooja
 */
public class Lab5InvertedIndex {
   

    public static class InvertedMapper extends Mapper<Object, Text, Text, Text>
    {
        
         private Text tag = new Text();
          private Text answerId = new Text();     
        public void map(Object key, Text values, Context context)
        {
             try {
                 String[] tokens = values.toString().split(",");
                 answerId.set(tokens[2]);
                 tag.set(tokens[1]);
                 
                 context.write(tag,answerId);
             } catch (IOException | InterruptedException ex) {
                 System.out.println("Erorr in Mapper"+ex.getMessage());
             }
                    
        }
    }
    
    
    public static class InvertedReducer extends Reducer<Text,Text,Text,Text>{
        
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            
            for(Text id: values){
                if(first){
                    first= false;
                }
                else {
                    sb.append("     ");
                }
                sb.append(id.toString());
            }
                result.set(sb.toString());
                context.write(key, result);
        }
        
        
        
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
         try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Inverted Index");
            job.setJarByClass(Lab5InvertedIndex.class);
            job.setMapperClass(InvertedMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(InvertedReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            TextOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException ex) {
            System.out.println("Error in Main" + ex.getMessage());
        }
    }
    }
    
