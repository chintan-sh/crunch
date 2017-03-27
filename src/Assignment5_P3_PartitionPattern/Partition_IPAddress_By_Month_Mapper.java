/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P3_PartitionPattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author Chintan
 */
public class Partition_IPAddress_By_Month_Mapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable createMonth = new IntWritable();
    private final static SimpleDateFormat fmt = new SimpleDateFormat("dd/MMM/yyyy");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] ipInfo = value.toString().split("-");
        String timing = ipInfo[2].trim();
        String[] cDate = timing.split(":");

        String createDate = cDate[0].substring(1, cDate[0].length()).trim();//System.out.println("Date found : '" + createDate + "'");

        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(fmt.parse(createDate)); //System.out.println("Month evaluated by format : " + fmt.parse(createDate)); //System.out.println("Month evaluated by cal : " + cal.get(Calendar.MONTH));
            createMonth.set(cal.get(Calendar.MONTH));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //System.out.println("CREATE MONTH " + createMonth);
        context.write(createMonth, value);
    }
}
