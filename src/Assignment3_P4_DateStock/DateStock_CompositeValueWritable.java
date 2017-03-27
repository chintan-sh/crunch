

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment3_P4_DateStock;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 *
 * @author chintan
 */
public class DateStock_CompositeValueWritable implements Writable {
    private String date;
    private String volume;
    private String maxStockPrice;
    
    public DateStock_CompositeValueWritable(){
        
    }
    
    public DateStock_CompositeValueWritable(String date, String volume, String maxStockPrice){
        this.date = date;
        this.volume= volume;
        this.maxStockPrice = maxStockPrice;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getMaxStockPrice() {
        return maxStockPrice;
    }

    public void setMaxStockPrice(String maxStockPrice) {
        this.maxStockPrice = maxStockPrice;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, date);
        WritableUtils.writeString(d, volume);
        WritableUtils.writeString(d, maxStockPrice);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        date = WritableUtils.readString(di);
        volume = WritableUtils.readString(di);
        maxStockPrice = WritableUtils.readString(di);
    }

    public String toString(){
        return (new StringBuilder().append(date).append("\t").append(volume).append("\t").append(maxStockPrice).toString());
    }

    
}
