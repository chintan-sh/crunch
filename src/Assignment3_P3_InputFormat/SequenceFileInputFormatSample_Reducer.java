package Assignment3_P3_InputFormat;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author chintan
 */
public class SequenceFileInputFormatSample_Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterator<LongWritable> values, Context context) throws InterruptedException, IOException {
        long sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }

        LongWritable out = new LongWritable(sum);
        context.write(key, out);
    }
    
}
