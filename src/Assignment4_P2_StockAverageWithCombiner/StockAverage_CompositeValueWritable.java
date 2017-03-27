

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment4_P2_StockAverageWithCombiner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author chintan
 */
public class StockAverage_CompositeValueWritable implements Writable {
    private int count;
    private String average;

    public StockAverage_CompositeValueWritable(){

    }

    public StockAverage_CompositeValueWritable(int count, String average){
        this.count = count;
        this.average= average;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }


    public String getAverage() {
        return average;
    }

    public void setAverage(String average) {
        this.average = average;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeVInt(d, count);
        WritableUtils.writeString(d, average);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        count = WritableUtils.readVInt(di);
        average = WritableUtils.readString(di);
    }

    public String toString(){
        return (new StringBuilder().append(count).append("\t").append(average).toString());
    }

    
}
