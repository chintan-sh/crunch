/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment4_P2_StockAverageWithCombiner;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Chintan
 */
public class StockAverage_Mapper extends Mapper<Object, Text, Text, StockAverage_CompositeValueWritable> {
    private static StockAverage_CompositeValueWritable stockPriceAdjCompositeObj = new StockAverage_CompositeValueWritable();
    private Text year;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] stockInfo = value.toString().split(",");

        if(!stockInfo[1].trim().equals("stock_symbol")) {
            String [] dateParts = stockInfo[2].split("-");
            Text year = new Text(dateParts[0]);

            stockPriceAdjCompositeObj.setCount(1);
            stockPriceAdjCompositeObj.setAverage(stockInfo[8]);

            context.write(year, stockPriceAdjCompositeObj);
        }
    }
}
