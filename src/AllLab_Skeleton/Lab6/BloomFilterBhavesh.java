/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AllLab_Skeleton.Lab6;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author bhavesh
 */
public class BloomFilterBhavesh {
    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
		Funnel<Person> p = new Funnel<Person>() {

			public void funnel(Person person, Sink into) {
				// TODO Auto-generated method stub
				into.putInt(person.id).putString(person.firstName, Charsets.UTF_8)
						.putString(person.lastName, Charsets.UTF_8).putInt(person.birthYear);
			}


		};
		private BloomFilter<Person> friends = BloomFilter.create(p, 500, 0.1);

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			Person p1 = new Person(1, "Abby", "Lahm", 3);
			Person p2 = new Person(2, "Jamie", "Scott", 4);
			ArrayList<Person> friendsList = new ArrayList<Person>();
			friendsList.add(p1);
			friendsList.add(p2);

			for (Person pr : friendsList) {
				friends.put(pr);
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String values[] = value.toString().split(",");
			Person p = new Person(Integer.parseInt(values[0]), values[1], values[2], Integer.parseInt(values[3]));
			if (friends.mightContain(p)) {
				context.write(value, NullWritable.get());
			}

		}

	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Bloom Filter");
		job.setJarByClass(BloomFilterBhavesh.class);
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.out.println(success);

	}
}
