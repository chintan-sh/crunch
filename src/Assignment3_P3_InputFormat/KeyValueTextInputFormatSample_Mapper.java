package Assignment3_P3_InputFormat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author chintan
 */
public class KeyValueTextInputFormatSample_Mapper extends Mapper<Object,Text,Text,Text> {
    @Override
    public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        context.write(new Text(key.toString()), new Text(value.toString()));
    }
}
