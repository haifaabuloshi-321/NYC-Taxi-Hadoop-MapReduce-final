
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

public class TripsByVendor {

    // Mapper: Extract vendor ID and emit (vendor, 1)
    public static class VendorMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final Text vendorKey = new Text();
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

            String vendor = fields[1];

            if (vendor.equals("1") || vendor.equals("2")) {
                vendorKey.set(vendor);
                context.write(vendorKey, one);
            }
        }
    }

    // Reducer: Calculate total trips per vendor
    public static class VendorReducer
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
                "Usage: TripsByVendor <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "NYC Taxi Trips By Vendor"
        );

        job.setJarByClass(TripsByVendor.class);

        job.setMapperClass(VendorMapper.class);
        job.setCombinerClass(VendorReducer.class);
        job.setReducerClass(VendorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
