package Assignment5_P5_InnerJoinPattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Chintan
 */
public class Assignment5_Part5_InnerJoin extends Configured implements Tool {

    public static class JoinMapper1 extends Mapper<Object,Text,Text,Text>
    {
        private Text outKey=new Text();
        private Text outValue=new Text();

        public void map(Object key, Text value,Context context)
        {
            try {
                String[] separatedInput = value.toString().split(";");
                String Id = separatedInput[0];
                if (!Id.equals("User-ID")) {
                    if (Id == null)
                        return;

                    outKey.set(Id);
                    outValue.set("A" + value);

                    context.write(outKey, outValue);
                }
            }
            catch (IOException | InterruptedException ex)
            {
                Logger.getLogger(Assignment5_Part5_InnerJoin.class.getName()).log(Level.SEVERE,null,ex);
            }
        }
    }

    public static class JoinMapper2 extends Mapper<Object,Text,Text,Text>
    {
        private Text outKey=new Text();
        private Text outValue=new Text();

        public void map(Object key, Text value,Context context)
        {
            try {
                String[] separatedInput = value.toString().split(";");
                String Id = separatedInput[0].trim();

                if (!Id.equals("User-ID")) {
                    if (Id == null)
                        return;

                    outKey.set(Id);
                    outValue.set("B" + value);

                    context.write(outKey, outValue);
                }
            }
            catch (IOException | InterruptedException ex)
            {
                Logger.getLogger(Assignment5_Part5_InnerJoin.class.getName()).log(Level.SEVERE,null,ex);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text,Text,Text,Text> {
        private static final Text EMPTY_TEXT = new Text();
        private Text temp = new Text();

        private ArrayList<Text> listA = new ArrayList<>();
        private ArrayList<Text> listB = new ArrayList<>();

        private String joinType = null;

        public void setup(Context context) {
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> value, Context context) {
            listA.clear();
            listB.clear();

            while (value.iterator().hasNext()) {
                temp = value.iterator().next();

                if (temp.charAt(0) == 'A')
                    listA.add(new Text(temp.toString().substring(1)));
                else if (temp.charAt(0) == 'B')
                    listB.add(new Text(temp.toString().substring(1)));
            }

            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) {
            if (joinType.equalsIgnoreCase("innerJoin")) {
                if (!listB.isEmpty() && !listA.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            try {
                                context.write(A, B);
                            } catch (IOException | InterruptedException ex) {
                                Logger.getLogger(Assignment5_Part5_InnerJoin.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf,"Inner");
        job.setJarByClass(Assignment5_Part5_InnerJoin.class);

        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,JoinMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,JoinMapper2.class);

        job.getConfiguration().set("join.type","innerJoin");

        job.setReducerClass(JoinReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success=job.waitForCompletion(true);

        return success?0:2;
    }
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        try {
            int res= ToolRunner.run(new Configuration(), new Assignment5_Part5_InnerJoin(),args);
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Assignment5_Part5_InnerJoin.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
