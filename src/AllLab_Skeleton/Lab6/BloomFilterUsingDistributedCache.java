package AllLab_Skeleton.Lab6;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import org.apache.hadoop.filecache.DistributedCache;

public class BloomFilterUsingDistributedCache {

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {

        Funnel<Person> p = new Funnel<Person>() {

            public void funnel(Person person, Sink into) {
                into.putInt(person.id).putString(person.firstName, Charsets.UTF_8)
                        .putString(person.lastName, Charsets.UTF_8).putInt(person.birthYear);
            }
        };

        private BloomFilter<Person> friends = BloomFilter.create(p, 500, 0.01);

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            int id;
            String fname;
            String lname;
            int byear;
            
            try {
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (files != null && files.length > 0) {

                    for (Path file : files) {

                        try {
                            File myFile = new File(file.toUri());
                            BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
                            String line = null;
                            while ((line = bufferedReader.readLine()) != null) {
                                String[] split = line.split(",");

                                id = Integer.parseInt(split[0]);
                                fname = String.valueOf(split[1]);
                                lname = String.valueOf(split[2]);
                                byear = Integer.parseInt(split[3]);
                                Person p = new Person(id, fname, lname, byear);
                                friends.put(p);
                            }
                        } catch (IOException ex) {
                            System.err.println("Exception while reading  file: " + ex.getMessage());
                        }
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
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
        job.setJarByClass(BloomFilterUsingDistributedCache.class);
        job.setMapperClass(BloomFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //adding the file in the cache having the Person class records
        //job.addCacheFile(new Path("localhost:9000/bhavesh/LabAssignment/CacheInput/cache.txt").toUri());
        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
