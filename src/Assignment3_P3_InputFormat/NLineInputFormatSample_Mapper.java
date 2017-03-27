package Assignment3_P3_InputFormat;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author chintan
 */
public class NLineInputFormatSample_Mapper extends Mapper<Object,Text,IntWritable,IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] separatedString = value.toString().split("::");
        String userID = separatedString[0];
    
        context.write(new IntWritable(Integer.parseInt(userID)), one);
       
    }
    
}
