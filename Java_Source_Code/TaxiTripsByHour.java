
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaxiTripsByHour {

    // Mapper: Extract pickup hour and emit (hour, 1)
    public static class TripMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text hourKey = new Text();
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

            // Validate number of columns
            if (fields.length != 11) {
                return;
            }

            String pickupDateTime = fields[2];

            try {
                // Pickup hour is at positions 11 and 12
                if (pickupDateTime.length() >= 13) {
                    String hour = pickupDateTime.substring(11, 13);
                    int hourNumber = Integer.parseInt(hour);

                    if (hourNumber >= 0 && hourNumber <= 23) {
                        hourKey.set(hour);
                        context.write(hourKey, one);
                    }
                }
            } catch (NumberFormatException e) {
                // Ignore invalid timestamps
            }
        }
    }

    // Reducer: Sum all trips for each hour
    public static class TripReducer
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

    // Driver: Configure and execute Hadoop job
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: TaxiTripsByHour <input path> <output path>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "NYC Taxi Trips Count By Pickup Hour"
        );

        job.setJarByClass(TaxiTripsByHour.class);

        job.setMapperClass(TripMapper.class);
        job.setCombinerClass(TripReducer.class);
        job.setReducerClass(TripReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
