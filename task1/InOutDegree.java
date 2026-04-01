import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InOutDegree {

    // ── Mapper ────────────────────────────────────────────────────────────────
    // Input:  each line "u v"  (one edge)
    // Output: (u, "OUT"), (v, "IN")
    public static class DegreeMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text vertexKey = new Text();
        private Text outLabel  = new Text("OUT");
        private Text inLabel   = new Text("IN");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;

            String u = parts[0];  // source vertex
            String v = parts[1];  // destination vertex

            // u has one more out-edge
            vertexKey.set(u);
            context.write(vertexKey, outLabel);

            // v has one more in-edge
            vertexKey.set(v);
            context.write(vertexKey, inLabel);
        }
    }

    // ── Reducer ───────────────────────────────────────────────────────────────
    // Input:  (vertex, ["IN", "OUT", "OUT", "IN", ...])
    // Output: (vertex, "IN=x OUT=y")
    public static class DegreeReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int inDegree  = 0;
            int outDegree = 0;

            for (Text val : values) {
                if (val.toString().equals("IN")) {
                    inDegree++;
                } else {
                    outDegree++;
                }
            }

            result.set("IN=" + inDegree + " OUT=" + outDegree);
            context.write(key, result);
        }
    }

    // ── Driver ────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: InOutDegree <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InOutDegree");

        job.setJarByClass(InOutDegree.class);
        job.setMapperClass(DegreeMapper.class);
        job.setReducerClass(DegreeReducer.class);

        // NO combiner in this version

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
