package Section08;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.HashSet;

public class UniqueListener {

    public static class MusicDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text trackId = new Text();
        private final static IntWritable one = new IntWritable(1);
        private ArrayList<String> user_ids;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            ArrayList<String> user_ids = new ArrayList<String>();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\|");

            user_ids.add(fields[0]);

            // context.write(new Text("unique_listeners"), new
            // IntWritable(Integer.parseInt(fields[0])));

            context.write(new Text("track_shares"), new IntWritable(Integer.parseInt(fields[2])));
            context.write(new Text("radio_listens"), new IntWritable(Integer.parseInt(fields[3])));
            context.write(new Text("radio_skips"), new IntWritable(Integer.parseInt(fields[4])));
            context.write(trackId, new IntWritable(1));
            if (Integer.parseInt(fields[3]) == 1 && Integer.parseInt(fields[4]) == 1) {
                context.write(trackId, new IntWritable(1));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the graph
            HashSet<String> myHashSet = new HashSet<String>(user_ids);
            int count = myHashSet.size();

            context.write(new Text("Number of unique listeners"), new IntWritable(count));
        }

    }

    public static class MusicDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // if (key.compareTo(new Text("unique_listeners")) == 0) {
            // int sum = 0;
            // ArrayList<String> dynamicArray = new ArrayList<String>();

            // for (IntWritable value : values) {

            // dynamicArray.add(value.toString());
            // sum += 1;

            // }
            // context.write(key, new IntWritable(sum));
            // } else {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
            // }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Music Data Analysis");
        job.setJarByClass(UniqueListener.class);
        job.setMapperClass(MusicDataMapper.class);
        job.setCombinerClass(MusicDataReducer.class);
        job.setReducerClass(MusicDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
