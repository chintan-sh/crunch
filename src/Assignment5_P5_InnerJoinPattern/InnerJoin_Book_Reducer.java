/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Assignment5_P5_InnerJoinPattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class InnerJoin_Book_Reducer extends Reducer<Text, Text, Text, Text> {

    private StringBuilder sb = new StringBuilder();
    private int count = 0;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        context.write(null, new Text("<MovieAndGenre>"));
    }


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.append("<Movie>");

        for (Text value : values){
            if(value.toString().charAt(0) == 'T' && count < 1){
                String title = value.toString().substring(1, value.toString().length()).trim();
                // put it in a tag
                sb.append("<Title>")
                        .append(title)
                        .append("</Title>");
                // in count so next time it doesnt come in
                count++;
            }else{
                String tag = value.toString().substring(1, value.toString().length()).trim();

                // call genre fellow
                constructPropertyXml(tag);
            }
        }

        sb.append("</Movie>");
        context.write(null, new Text(sb.toString()));
    }

    public void constructPropertyXml(String tag) {
        sb.append("<Genre>").append(tag)
                .append("</Genre>");
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        context.write(null, new Text("</MovieAndGenre>"));
    }
}
