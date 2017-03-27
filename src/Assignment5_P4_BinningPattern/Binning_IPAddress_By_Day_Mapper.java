/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P4_BinningPattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author Chintan
 */
public class Binning_IPAddress_By_Day_Mapper extends Mapper<Object, Text, NullWritable, Text> {
    private IntWritable createHour = new IntWritable();
    private final static SimpleDateFormat fmt = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
    private MultipleOutputs<NullWritable, Text> multipleOutputs;


    public void setup(Context context){
        multipleOutputs = new MultipleOutputs(context);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] ipInfo = value.toString().split("-");
        String timing = ipInfo[2].trim();

        String createDate = timing.substring(1, timing.length()).trim();//System.out.println("Date found : '" + createDate + "'");

        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(fmt.parse(createDate)); //System.out.println("Month evaluated by format : " + fmt.parse(createDate)); //System.out.println("Month evaluated by cal : " + cal.get(Calendar.MONTH));
            createHour.set(cal.get(Calendar.HOUR_OF_DAY));
            multipleOutputs.write("textualBins", NullWritable.get(), value, createHour+"-hour");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        multipleOutputs.close();
    }
}
