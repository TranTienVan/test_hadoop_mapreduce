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

public class MusicDataAnalysis {

    public static class MusicDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text trackId = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\|");
            trackId.set(fields[1]);
            context.write(new Text("unique_listeners"), new IntWritable(Integer.parseInt(fields[0])));
            context.write(new Text("track_shares"), new IntWritable(Integer.parseInt(fields[2])));
            context.write(new Text("radio_listens"), new IntWritable(Integer.parseInt(fields[3])));
            context.write(new Text("radio_skips"), new IntWritable(Integer.parseInt(fields[4])));
            context.write(trackId, new IntWritable(1));
            if (Integer.parseInt(fields[3]) == 1 && Integer.parseInt(fields[4]) == 1) {
                context.write(trackId, new IntWritable(1));
            }
        }
    }

    public static class MusicDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            if (key.compareTo(new Text("unique_listeners")) == 0) {
                int sum = 0;
                ArrayList<Integer> dynamicArray = new ArrayList<Integer>();

                for (IntWritable value : values) {
                    if (!dynamicArray.contains(value.get())) {
                        dynamicArray.add(value.get());
                        sum += 1;
                    }
                }
                context.write(key, new IntWritable(sum));
            } else {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                context.write(key, new IntWritable(sum));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Music Data Analysis");
        job.setJarByClass(MusicDataAnalysis.class);
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
