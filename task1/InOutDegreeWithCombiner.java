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

import java.io.IOException;

public class InOutDegreeWithCombiner {

    // ── Mapper ────────────────────────────────────────────────────────────────
    // Input:  each line "u v"
    // Output: ("u_OUT", 1), ("v_IN", 1)
    // Using composite key so combiner can sum partial counts
    public static class DegreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text compositeKey    = new Text();
        private IntWritable one      = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;

            String u = parts[0];
            String v = parts[1];

            // source vertex: out-degree
            compositeKey.set(u + "_OUT");
            context.write(compositeKey, one);

            // destination vertex: in-degree
            compositeKey.set(v + "_IN");
            context.write(compositeKey, one);
        }
    }

    // ── Combiner ──────────────────────────────────────────────────────────────
    // Runs locally on each mapper output BEFORE shuffle
    // Reduces ("u_OUT", [1,1,1,...]) -> ("u_OUT", partial_sum)
    // This is the key difference vs the no-combiner version:
    // much less data transferred over the network during shuffle
    public static class DegreeCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable partialSum = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            partialSum.set(sum);
            context.write(key, partialSum);
        }
    }

    // ── Reducer ───────────────────────────────────────────────────────────────
    // Input:  ("u_OUT", [partial_sum1, partial_sum2, ...])
    //         ("u_IN",  [partial_sum1, ...])
    // Output: (vertex, "IN=x OUT=y")
    public static class DegreeReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Text vertexKey = new Text();
        private Text result    = new Text();

        // We accumulate per-vertex counts across IN and OUT keys
        // But since keys arrive sorted, we handle them key by key
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int total = 0;
            for (IntWritable val : values) {
                total += val.get();
            }

            // key format: "vertexId_IN" or "vertexId_OUT"
            String keyStr = key.toString();
            int sep = keyStr.lastIndexOf('_');
            String vertex = keyStr.substring(0, sep);
            String type   = keyStr.substring(sep + 1);

            // Emit each (vertex_TYPE, count) as its own line
            // Post-processing can join IN and OUT per vertex if needed
            vertexKey.set(vertex);
            result.set(type + "=" + total);
            context.write(vertexKey, result);
        }
    }

    // ── Driver ────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: InOutDegreeWithCombiner <input> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InOutDegreeWithCombiner");

        job.setJarByClass(InOutDegreeWithCombiner.class);
        job.setMapperClass(DegreeMapper.class);
        job.setCombinerClass(DegreeCombiner.class);   // <-- combiner enabled
        job.setReducerClass(DegreeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
