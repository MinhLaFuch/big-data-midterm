import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Task1b_DegreeCounting {

    public static void generateEdgeList(String outputPath, int numVertices, long numEdges) throws IOException {
        System.out.println("Generating " + numEdges + " edges, " + numVertices + " vertices...");
        Random random = new Random(42);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath), 1 << 16);
        long count = 0;
        while (count < numEdges) {
            int src = random.nextInt(numVertices);
            int dst = random.nextInt(numVertices);
            if (src != dst) {
                writer.write(src + " " + dst + "\n");
                count++;
            }
        }
        writer.close();
        System.out.println("Done. Saved to: " + outputPath);
    }

    // Mapper: emit (vertex, 1) for OUT and (vertex, -1) for IN
    // Using LongWritable avoids any Text parsing issues entirely
    public static class DegreeMapper extends Mapper<Object, Text, Text, LongWritable> {
        private Text vertexKey = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\\s+");
            if (parts.length != 2) return;

            // src: outgoing edge -> emit +1
            vertexKey.set(parts[0] + "_OUT");
            context.write(vertexKey, new LongWritable(1));

            // dst: incoming edge -> emit +1
            vertexKey.set(parts[1] + "_IN");
            context.write(vertexKey, new LongWritable(1));
        }
    }

    // Combiner: sum counts locally
    public static class DegreeCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) sum += val.get();
            context.write(key, new LongWritable(sum));
        }
    }

    // Reducer: sum all counts, format back to "in=X" or "out=X"
    // setMapOutputKeyClass/ValueClass tells Hadoop the combiner types separately
    // so reducer can safely output <Text, Text> without breaking the combiner
    public static class DegreeReducer extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) sum += val.get();
            String k = key.toString();
            if (k.endsWith("_IN")) {
                context.write(new Text(k.replace("_IN", "")), new Text("in=" + sum));
            } else {
                context.write(new Text(k.replace("_OUT", "")), new Text("out=" + sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("generate")) {
            generateEdgeList("edges.txt", 100000, 1000000000L);
            return;
        }

        if (args.length < 2) {
            System.err.println("Usage: Task1b_DegreeCounting <input> <o> [--no-combiner]");
            System.exit(1);
        }

        boolean useCombiner = !(args.length == 3 && args[2].equals("--no-combiner"));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DegreeCounting combiner=" + useCombiner);
        job.setJarByClass(Task1b_DegreeCounting.class);
        job.setMapperClass(DegreeMapper.class);
        job.setReducerClass(DegreeReducer.class);

        if (useCombiner) {
            job.setCombinerClass(DegreeCombiner.class);
            System.out.println(">> Running WITH combiner");
        } else {
            System.out.println(">> Running WITHOUT combiner");
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long start = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        System.out.println("Job finished in: " + (System.currentTimeMillis() - start) + " ms");
        System.exit(success ? 0 : 1);
    }
}