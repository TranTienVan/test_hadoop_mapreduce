package Section08;

import org.apache.hadoop.mapreduce.Job;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueListener {
    public static class LastFMConstants {
        public static final int USER_ID = 0;
        public static final int TRACK_ID = 1;
        public static final int IS_SHARED = 2;
        public static final int RADIO = 3;
        public static final int IS_SKIPPED = 4;
    }

    public static class UniquePairsReducer extends Reducer<Text, Text, Text, Text> {

        private Text outputValue = new Text("");

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            context.write(key, outputValue);
        }
    }

    public static class UniquePairsMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split("\\|");
            String userId = fields[0];
            String trackId = fields[1];

            outputKey.set(userId + "|" + trackId);
            outputValue.set("");

            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: UniquePairsDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(UniqueListener.class);
        job.setJobName("UniquePairs");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(UniquePairsMapper.class);
        job.setReducerClass(UniquePairsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
