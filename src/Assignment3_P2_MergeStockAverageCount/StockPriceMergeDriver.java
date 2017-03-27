/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P2_MergeStockAverageCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class StockPriceMergeDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        // local file system handle
        FileSystem local = FileSystem.getLocal(conf);

        // hdfs file system handle
        FileSystem hdfs = FileSystem.get(conf);

        // local input directory
        Path inputDir = new Path(args[0]);

        // hdfs i/p  directory
        Path inputDir1 = new Path(args[1]);

        // local input files in local dir
        FileStatus[] inputFiles = local.listStatus(inputDir);

        // o/p stream
        FSDataOutputStream out = hdfs.create(inputDir1);

        // open each file and extract contents of file
        for(int i=0; i < inputFiles.length; i++) {
            System.out.println("File name ----------------------------------------------------------------> " + inputFiles[i].getPath().getName());
            FSDataInputStream in = local.open(inputFiles[i].getPath());
            byte buffer[] = new byte[256];
            int bytesRead = 0;

            // extract all contents of file
            while((bytesRead = in.read(buffer))>0){
                out.write(buffer, 0, bytesRead);
            }

            // close input stream
            in.close();
        }

        Job job = Job.getInstance(conf, "Average Stock Price");
        job.setJarByClass(StockPriceMergeDriver.class);
        job.setMapperClass(StockPriceMerge_Mapper.class);
        job.setCombinerClass(StockPriceMerge_Reducer.class);
        job.setReducerClass(StockPriceMerge_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1])); // above programs output will be input for mapper
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
