/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AllLab_Skeleton.Lab2;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pooja
 */
public class Lab2Mapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable> {
    
        @Override
        public void map(Object key, Text values, Context context)
        {
            if (values.toString().length()>0)
            {
                try {
                    String value[] = values.toString().split("\t");
                    CompositeKeyWritable cw = new CompositeKeyWritable(value[6],value[3]);
                    context.write(cw, NullWritable.get());
                } catch (IOException | InterruptedException ex) {
                    Logger.getLogger(Lab2Mapper.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    
}
