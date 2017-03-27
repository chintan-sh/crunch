/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P2_MergeStockAverageCount;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class StockPriceMerge_Mapper extends Mapper<Object, Text, Text, FloatWritable> {
    private static FloatWritable highestPrice;
    private Text stockName;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("Content on line " + key.toString() + " :- " + value.toString() );

        String[] stockInfo = value.toString().split(",");
        //System.out.println("String is split now ");

        if(!stockInfo[1].trim().equals("stock_symbol")) {
            Text stockName = new Text(stockInfo[1]);
            //System.out.println("Stockname extracted " + stockName.toString());

            FloatWritable highestPrice = new FloatWritable(Float.parseFloat(stockInfo[4]));
            //System.out.println("So is the price " + highestPrice);

            context.write(stockName, highestPrice);
        }
    }
}


