package Section07;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import Section07.EncryptModule;

public class DeIdentifyData {

    public static class PatientDataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input record by comma
            String[] fields = value.toString().split(",");
            EncryptModule encrypt_moduler = new EncryptModule();
            // Construct the output value as comma-separated fields
            String outputValue = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s",
                    encrypt_moduler.encrypt(fields[0]),
                    encrypt_moduler.encrypt(fields[1]),
                    encrypt_moduler.encrypt(fields[2]),
                    encrypt_moduler.encrypt(fields[3]),
                    encrypt_moduler.encrypt(fields[4]),
                    encrypt_moduler.encrypt(fields[5]),
                    encrypt_moduler.encrypt(fields[6]),
                    encrypt_moduler.encrypt(fields[7]),
                    encrypt_moduler.encrypt(fields[8]));

            // Write the output value to the reducer
            context.write(NullWritable.get(), new Text(outputValue));
        }
    }

    public static class PatientDataReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Loop through the input values and write each one to the output
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Patient Data Processor");
        job.setJarByClass(DeIdentifyData.class);
        job.setMapperClass(PatientDataMapper.class);
        job.setReducerClass(PatientDataReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
