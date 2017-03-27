/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P2_DistinctIPAddress;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class DistinctIPAddress_Mapper extends Mapper<Object, Text, Text, NullWritable> {

    private Text ipaddr;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] ipInfo = value.toString().split("-");
        ipaddr = new Text(ipInfo[0].trim());
        context.write(ipaddr, NullWritable.get());
    }
}
