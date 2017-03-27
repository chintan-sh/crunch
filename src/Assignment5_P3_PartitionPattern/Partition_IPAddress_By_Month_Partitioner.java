/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P3_PartitionPattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author Chintan
 */
public class Partition_IPAddress_By_Month_Partitioner extends Partitioner<IntWritable, Text>{
    private int minMonth = 0;


    @Override
    public int getPartition(IntWritable key, Text value, int numOfPartitions) {
        return (key.get() - minMonth);
    }
}
