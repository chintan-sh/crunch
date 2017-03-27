package Assignment3_P3_InputFormat;

/**
 * Created by chintan on 2/12/17.
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CombineFileInputFormatSample extends Configured implements Tool {

    public static class CombineMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
            String val = value.toString();
            String[] var = val.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            if (!(var[0].equals("Name"))) {
                context.write(new Text(var[0]), new Text(var[2]));
            }
        }
    }

    public static class CombineReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            for (Text values : value){
                context.write(key, values);
            }
        }
    }

    public static class comInputFormat extends CombineFileInputFormat<LongWritable,Text>{
        @Override
        public RecordReader<LongWritable,Text> createRecordReader(InputSplit split,TaskAttemptContext context)  throws IOException{
            CombineFileRecordReader<LongWritable,Text> r1 = new CombineFileRecordReader<LongWritable,Text>((CombineFileSplit) split, context, comFileRecordReader.class);
            return r1;
        }
    }

    public static class comFileRecordReader extends RecordReader<LongWritable,Text>{
        private LineRecordReader lineRecordReader = new LineRecordReader();

        public comFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index)
                throws IOException{
            FileSplit fs = new FileSplit(split.getPath(index),split.getOffset(index), split.getLength(index), split.getLocations());
            lineRecordReader.initialize(fs, context);
        }


        @Override
        public void initialize(InputSplit split,TaskAttemptContext context)
                throws IOException, InterruptedException{
            //
        }

        @Override
        public void close() throws IOException{
            lineRecordReader.close();
        }

        @Override
        public float getProgress() throws IOException{
            return lineRecordReader.getProgress();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException{
            return lineRecordReader.getCurrentKey();
        }

        @Override
        public Text getCurrentValue() throws IOException,InterruptedException{
            return lineRecordReader.getCurrentValue();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException{
            return lineRecordReader.nextKeyValue();
        }

    }
    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "1048576"); //1MB

        Job job = new Job(conf,"Combine files input format sample");
        job.setJarByClass(CombineFileInputFormatSample.class);

        job.setMapperClass(CombineMapper.class);
        job.setReducerClass(CombineReducer.class);
        job.setInputFormatClass(comInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exit = ToolRunner.run(new CombineFileInputFormatSample(), args);
        System.exit(exit);
    }

}