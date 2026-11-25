
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Stores both sum of durations and count of trips
public class AverageDurationByHour {

    public static class SumCount implements Writable {
        private long sum;
        private long count;

        public SumCount() {
            this.sum = 0;
            this.count = 0;
        }

        public SumCount(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public long getCount() {
            return count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(sum);
            out.writeLong(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readLong();
            count = in.readLong();
        }
    }

    // Mapper
    public static class DurationMapper
            extends Mapper<Object, Text, Text, SumCount> {

        private final Text hourKey = new Text();

        @Override
        protected void map(
                Object key,
                Text value,
                Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("id,")) {
                return;
            }

            String[] fields = line.split(",", -1);

            if (fields.length != 11) {
                return;
            }

            try {
                String pickupTime = fields[2];
                String hour = pickupTime.substring(11, 13);

                long duration = Long.parseLong(fields[10]);

                int hourNumber = Integer.parseInt(hour);

                if (hourNumber >= 0 && hourNumber <= 23 && duration > 0) {
                    hourKey.set(hour);
                    context.write(hourKey, new SumCount(duration, 1));
                }

            } catch (Exception e) {
                // Ignore invalid records
            }
        }
    }

    // Combiner and Reducer use the same aggregation logic
    public static class SumCountReducer
            extends Reducer<Text, SumCount, Text, SumCount> {

        @Override
        protected void reduce(
                Text key,
                Iterable<SumCount> values,
                Context context
        ) throws IOException, InterruptedException {

            long totalSum = 0;
            long totalCount = 0;

            for (SumCount value : values) {
                totalSum += value.getSum();
                totalCount += value.getCount();
            }

            context.write(key, new SumCount(totalSum, totalCount));
        }
    }

    // Final reducer: calculate average duration
    public static class AverageReducer
            extends Reducer<Text, SumCount, Text, Text> {

        @Override
        protected void reduce(
                Text key,
                Iterable<SumCount> values,
                Context context
        ) throws IOException, InterruptedException {

            long totalSum = 0;
            long totalCount = 0;

            for (SumCount value : values) {
                totalSum += value.getSum();
                totalCount += value.getCount();
            }

            if (totalCount > 0) {
                double average = (double) totalSum / totalCount;

                context.write(
                    key,
                    new Text(String.format("%.2f", average))
                );
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: AverageDurationByHour <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Average Taxi Trip Duration By Pickup Hour"
        );

        job.setJarByClass(AverageDurationByHour.class);

        job.setMapperClass(DurationMapper.class);
        job.setCombinerClass(SumCountReducer.class);
        job.setReducerClass(AverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
