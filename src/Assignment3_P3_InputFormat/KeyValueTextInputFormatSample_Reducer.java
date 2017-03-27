package Assignment3_P3_InputFormat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author chintan
 */
public class KeyValueTextInputFormatSample_Reducer extends Reducer <Text, Text, Text, Text> {
 
    public void reduce(Text key, Text values, Context context) throws InterruptedException, IOException {
		context.write(key, values);
	}
}
