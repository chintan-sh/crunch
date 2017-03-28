/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P6_StructureToHierarchyPattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author Chintan
 */
public class Structure_Hierarchy_Tag_Mapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] movieInfo = value.toString().split(",");
        String movieId = movieInfo[1];

        //append 'A' for tag identification
        String movieTag = "A" + movieInfo[2];

        // movieID, movieTag
        context.write(new Text(movieId), new Text(movieTag));
    }
}
