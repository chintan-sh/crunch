/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P6_StructureToHierarchyPattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Structure_HierarchyDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Structure to Hierarchy");
        job.setJarByClass(Structure_HierarchyDriver.class);

        // pass file 1 to this mapper in Text format
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Structure_Hierarchy_Movie_Mapper.class);

        // pass file 2 to this mapper in Text format
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Structure_Hierarchy_Tag_Mapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Structure_Hierarchy_Reducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
