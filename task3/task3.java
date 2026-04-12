import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

/**
 * EXAMPLE: PCY Algorithm on a movie ratings dataset
 * 
 * Input (baskets.csv equivalent): user_id, movie, genre
 * A "basket" = all movies watched by one user in one session
 *
 * PCY runs in 2 passes:
 *   Pass 1 - Count individual items AND hash pairs into buckets
 *   Pass 2 - Count only pairs whose bucket was frequent (bitmap filter)
 *
 * This is analogous to your Task 3 on baskets.csv.
 */
public class task3 {

    static final int SUPPORT_THRESHOLD = 3;   // minimum support to be "frequent"
    static final int NUM_BUCKETS = 100;        // number of hash buckets

    // SHARED UTILITY: Hash function for a pair of items
    // A good hash spreads pairs evenly across buckets
    static int hashPair(String itemA, String itemB, int numBuckets) {
        // Use a polynomial hash of the combined string
        String combined = itemA.compareTo(itemB) < 0
            ? itemA + "|" + itemB
            : itemB + "|" + itemA;
        int hash = 0;
        for (char c : combined.toCharArray()) {
            hash = (hash * 31 + c) % numBuckets;
        }
        return Math.abs(hash);
    }

    // PASS 1: Count items and hash pairs into buckets
    public static class PCYPass1 {

        // Mapper: emit (item, 1) for each item AND ("BUCKET_X", 1) for each pair
        public static class Pass1Mapper extends Mapper<Object, Text, Text, IntWritable> {

            private static final IntWritable ONE = new IntWritable(1);

            @Override
            public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {

                // Input line: "userID,movie1,movie2,movie3,..."
                String[] parts = value.toString().split(",");
                if (parts.length < 2) return;

                List<String> items = new ArrayList<>();
                for (int i = 1; i < parts.length; i++) {
                    items.add(parts[i].trim());
                }

                // Count each individual item
                for (String item : items) {
                    context.write(new Text("ITEM_" + item), ONE);
                }

                // Hash each pair into a bucket
                for (int i = 0; i < items.size(); i++) {
                    for (int j = i + 1; j < items.size(); j++) {
                        int bucket = hashPair(items.get(i), items.get(j), NUM_BUCKETS);
                        context.write(new Text("BUCKET_" + bucket), ONE);
                    }
                }
            }
        }

        // Combiner: pre-aggregate counts locally before sending to reducer
        // This is optional but greatly reduces network traffic
        public static class Pass1Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {

            @Override
            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) sum += val.get();
                context.write(key, new IntWritable(sum));
            }
        }

        // Reducer: sum up counts for each item and each bucket
        // Output: "ITEM_X  count" or "BUCKET_X  count"
        public static class Pass1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

            @Override
            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) sum += val.get();

                // Only output frequent items and frequent buckets
                if (sum >= SUPPORT_THRESHOLD) {
                    context.write(key, new IntWritable(sum));
                }
            }
        }

        // Wire up Pass 1 job
        public static Job createJob(String input, String output, boolean useCombiner)
                throws IOException {
            Configuration conf = new Configuration();
            conf.setInt("support.threshold", SUPPORT_THRESHOLD);
            conf.setInt("num.buckets", NUM_BUCKETS);

            Job job = Job.getInstance(conf, "PCY Pass 1");
            job.setJarByClass(task3.class);

            job.setMapperClass(Pass1Mapper.class);
            job.setReducerClass(Pass1Reducer.class);
            if (useCombiner) job.setCombinerClass(Pass1Combiner.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            return job;
        }
    }

    // PASS 2: Count only candidate pairs
    // A pair (A, B) is a candidate if:
    //   1. Both A and B are frequent items (from Pass 1)
    //   2. Their hash bucket was frequent (from Pass 1)
    public static class PCYPass2 {

        // Mapper: re-read baskets, only emit pairs that pass the bitmap filter
        // The frequent items and bitmap are loaded from Pass 1 output
        public static class Pass2Mapper extends Mapper<Object, Text, Text, IntWritable> {

            private Set<String> frequentItems = new HashSet<>();
            private boolean[] frequentBuckets;  // bitmap
            private static final IntWritable ONE = new IntWritable(1);

            @Override
            protected void setup(Context context) throws IOException {
                // In real code, load frequentItems and frequentBuckets
                // from Pass 1 output using DistributedCache
                // For this example we just simulate it
                frequentBuckets = new boolean[NUM_BUCKETS];
                Arrays.fill(frequentBuckets, true); // simplified for example
            }

            @Override
            public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {

                String[] parts = value.toString().split(",");
                if (parts.length < 2) return;

                List<String> items = new ArrayList<>();
                for (int i = 1; i < parts.length; i++) {
                    String item = parts[i].trim();
                    // Only keep frequent items
                    if (frequentItems.isEmpty() || frequentItems.contains(item)) {
                        items.add(item);
                    }
                }

                // Only emit pairs that pass the bitmap filter
                for (int i = 0; i < items.size(); i++) {
                    for (int j = i + 1; j < items.size(); j++) {
                        String a = items.get(i);
                        String b = items.get(j);
                        int bucket = hashPair(a, b, NUM_BUCKETS);

                        // Bitmap check: only count if bucket was frequent
                        if (frequentBuckets[bucket]) {
                            String pair = a.compareTo(b) < 0
                                ? a + "|" + b
                                : b + "|" + a;
                            context.write(new Text(pair), ONE);
                        }
                    }
                }
            }
        }

        // Combiner: same pattern as Pass 1
        public static class Pass2Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {

            @Override
            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) sum += val.get();
                context.write(key, new IntWritable(sum));
            }
        }

        // Reducer: sum and filter by support threshold
        // Output: "itemA|itemB  count" for all frequent pairs
        public static class Pass2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

            private int supportThreshold;

            @Override
            protected void setup(Context context) {
                supportThreshold = context.getConfiguration().getInt("support.threshold", 3);
            }

            @Override
            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) sum += val.get();
                if (sum >= supportThreshold) {
                    context.write(key, new IntWritable(sum));
                }
            }
        }

        public static Job createJob(String input, String output, boolean useCombiner)
                throws IOException {
            Configuration conf = new Configuration();
            conf.setInt("support.threshold", SUPPORT_THRESHOLD);
            conf.setInt("num.buckets", NUM_BUCKETS);

            Job job = Job.getInstance(conf, "PCY Pass 2");
            job.setJarByClass(task3.class);

            job.setMapperClass(Pass2Mapper.class);
            job.setReducerClass(Pass2Reducer.class);
            if (useCombiner) job.setCombinerClass(Pass2Combiner.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            return job;
        }
    }

    // MAIN: Run Pass 1 then Pass 2
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: task3 <input> <pass1_output> <pass2_output> [--no-combiner]");
            System.exit(1);
        }

        boolean useCombiner = !(args.length == 4 && args[3].equals("--no-combiner"));
        System.out.println(">> Combiner: " + (useCombiner ? "ON" : "OFF"));

        // Run Pass 1
        long t1 = System.currentTimeMillis();
        Job pass1 = PCYPass1.createJob(args[0], args[1], useCombiner);
        pass1.waitForCompletion(true);
        long t2 = System.currentTimeMillis();
        System.out.println("Pass 1 time: " + (t2 - t1) + " ms");

        // Run Pass 2
        Job pass2 = PCYPass2.createJob(args[0], args[2], useCombiner);
        pass2.waitForCompletion(true);
        long t3 = System.currentTimeMillis();
        System.out.println("Pass 2 time: " + (t3 - t2) + " ms");
        System.out.println("Total time:  " + (t3 - t1) + " ms");
    }
}
