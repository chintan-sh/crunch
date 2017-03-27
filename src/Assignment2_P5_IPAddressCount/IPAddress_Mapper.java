/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment2_P5_IPAddressCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class IPAddress_Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text ipaddr;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("Content on line " + key.toString() + " :- " + value.toString() );

        String[] ipInfo = value.toString().split("-");
        //System.out.println("String is split now ");

        ipaddr = new Text(ipInfo[0].trim());
        //System.out.println("MovieId extracted " + movieID.toString());

        context.write(ipaddr, one);
    }
}
