package Assignment3_P3_InputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author chintan
 */
public class SequenceFileInputFormatSample_Mapper extends Mapper<Text, Text, Text, LongWritable> {
    final static Pattern alphanumeric_regex = Pattern.compile("\\w+");
    private final static LongWritable ONE = new LongWritable(1L);
    private Text single_word = new Text();

    public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
        Matcher matchObj = alphanumeric_regex.matcher(value.toString());
        while (matchObj.find()) {
            single_word.set(matchObj.group());
            context.write(single_word, ONE);
        }
    }
}
