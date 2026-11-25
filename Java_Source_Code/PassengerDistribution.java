
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PassengerDistribution {

    // Mapper: Extract passenger count and emit (count, 1)
    public static class PassengerMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text passengerKey = new Text();
        private final LongWritable one = new LongWritable(1);

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();

            // Skip CSV header
            if (line.startsWith("id,")) {
                return;
            }

            String[] fields = line.split(",", -1);

            if (fields.length != 11) {
                return;
            }

            try {
                int passengers = Integer.parseInt(fields[4]);

                if (passengers >= 0) {
                    passengerKey.set(String.valueOf(passengers));
                    context.write(passengerKey, one);
                }

            } catch (NumberFormatException e) {
                // Ignore invalid records
            }
        }
    }

    // Reducer: Sum trips for each passenger count
    public static class PassengerReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable result = new LongWritable();

        @Override
        protected void reduce(
                Text key,
                Iterable<LongWritable> values,
                Context context
        ) throws IOException, InterruptedException {

            long totalTrips = 0;

            for (LongWritable value : values) {
                totalTrips += value.get();
            }

            result.set(totalTrips);
            context.write(key, result);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: PassengerDistribution <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "NYC Taxi Passenger Distribution"
        );

        job.setJarByClass(PassengerDistribution.class);

        job.setMapperClass(PassengerMapper.class);
        job.setCombinerClass(PassengerReducer.class);
        job.setReducerClass(PassengerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
