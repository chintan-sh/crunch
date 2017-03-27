/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P4_DateStock;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class DateStock_Reducer extends Reducer<Text, DateStock_CompositeValueWritable, Text, Text> {
        private DateStock_CompositeValueWritable result = new DateStock_CompositeValueWritable();

        public void reduce(Text key, Iterable<DateStock_CompositeValueWritable> values, Context context) throws IOException, InterruptedException {
            int minStockVol = 0;
            int maxStockVol = 0;
            Float maxStockPriceAdj = 0f;
            String minDate = "";
            String maxDate = "";
            int count = 0;

            for(DateStock_CompositeValueWritable value : values){
                if(count < 1){
                    minStockVol = Integer.parseInt(value.getVolume());
                    minDate = value.getDate();
                    //System.out.println("Min date is " + minDate);
                }

                if(Integer.parseInt(value.getVolume()) < minStockVol){
                    minStockVol = Integer.parseInt(value.getVolume());
                    minDate = value.getDate();
                    //System.out.println("Min date is " + minDate);
                }

                if(Integer.parseInt(value.getVolume()) > maxStockVol){
                    maxStockVol = Integer.parseInt(value.getVolume());
                    maxDate = value.getDate();
                    //System.out.println("Max date is " + maxDate);
                }

                if(Float.parseFloat(value.getMaxStockPrice()) > maxStockPriceAdj){
                    maxStockPriceAdj = Float.parseFloat(value.getMaxStockPrice());
                }

                count++;
            }

            String op = minDate + "\t" + maxDate + "\t" + String.valueOf(maxStockPriceAdj);
            Text out = new Text(op);
            context.write(key, out);

        }
}
