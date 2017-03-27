/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P4_DateStock;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

//        exchange - 0
//        stock_symbol - 1
//        date - 2
//        stock_price_open -3
//        stock_price_high -4
//        stock_price_low -5
//        stock_price_close -6
//        stock_volume -7
//        stock_price_adj_close -8

/**
 *
 * @author Chintan
 */
public class DateStock_Mapper extends Mapper<Object, Text, Text, DateStock_CompositeValueWritable> {
    private Text stockName;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] stockInfo = value.toString().split(",");

        if(!stockInfo[1].trim().equals("stock_symbol")) {
            try {
                Text stockName = new Text(stockInfo[1]);
                DateStock_CompositeValueWritable dcvw = new DateStock_CompositeValueWritable(stockInfo[2],stockInfo[7], stockInfo[8]);
                context.write(stockName, dcvw);
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(DateStock_Mapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
