import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * EXAMPLE: City road network degree counting
 * Edge list format: "cityA cityB" meaning road from cityA to cityB
 *
 * This is analogous to your Task 1b which counts in/out degrees
 * of vertices in a directed graph.
 *
 * How to compile and run:
 *   javac -classpath $(hadoop classpath) -d . Task1b_DegreeCounting.java
 *   jar -cvf degrees.jar *.class
 *   hadoop jar degrees.jar Task1b_DegreeCounting <input> <output>
 */
public class Task1b_DegreeCounting {

    // -------------------------------------------------------------------------
    // STEP 1: Generate a random edge list and save to a text file
    // This simulates a directed graph (e.g. city roads, social network follows)
    // -------------------------------------------------------------------------
    public static void generateEdgeList(String outputPath, int numVertices, int numEdges) throws IOException {
        System.out.println("Generating edge list: " + numEdges + " edges, " + numVertices + " vertices...");
        Random random = new Random(42);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));

        int count = 0;
        while (count < numEdges) {
            int src = random.nextInt(numVertices);
            int dst = random.nextInt(numVertices);

            // No self-loops allowed
            if (src != dst) {
                // Format: "src dst"
                writer.write(src + " " + dst);
                writer.newLine();
                count++;
            }
        }

        writer.close();
        System.out.println("Done. Saved to: " + outputPath);
    }

    // -------------------------------------------------------------------------
    // STEP 2: Mapper
    // Input:  one line = "src dst"
    // Output: (src, "OUT") and (dst, "IN")
    //         We tag each vertex with whether it appears as source or destination
    // -------------------------------------------------------------------------
    public static class DegreeMapper extends Mapper<Object, Text, Text, Text> {

        private Text vertexKey = new Text();
        private Text degreeTag = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\s+");
            if (parts.length != 2) return;

            String src = parts[0];
            String dst = parts[1];

            // src has one outgoing edge
            vertexKey.set(src);
            degreeTag.set("OUT");
            context.write(vertexKey, degreeTag);

            // dst has one incoming edge
            vertexKey.set(dst);
            degreeTag.set("IN");
            context.write(vertexKey, degreeTag);
        }
    }

    // -------------------------------------------------------------------------
    // STEP 3: Combiner (optional, same class as Reducer here)
    // Runs locally on each mapper's output BEFORE sending to reducer
    // Reduces network traffic - very useful for large graphs
    // To test WITHOUT combiner: just remove the setCombinerClass line in main()
    // -------------------------------------------------------------------------
    public static class DegreeCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int inCount = 0;
            int outCount = 0;

            for (Text val : values) {
                if (val.toString().equals("IN")) inCount++;
                else if (val.toString().equals("OUT")) outCount++;
            }

            // Pass partial counts forward as tagged strings
            if (inCount > 0) {
                context.write(key, new Text("IN:" + inCount));
            }
            if (outCount > 0) {
                context.write(key, new Text("OUT:" + outCount));
            }
        }
    }

    // -------------------------------------------------------------------------
    // STEP 4: Reducer
    // Input:  (vertex, ["IN", "IN", "OUT", "OUT", "OUT", ...])
    //         OR if combiner was used: (vertex, ["IN:5", "OUT:3", ...])
    // Output: (vertex, "in=X out=Y")
    // -------------------------------------------------------------------------
    public static class DegreeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int inDegree = 0;
            int outDegree = 0;

            for (Text val : values) {
                String s = val.toString();

                if (s.startsWith("IN:")) {
                    // From combiner: already a partial count
                    inDegree += Integer.parseInt(s.substring(3));
                } else if (s.startsWith("OUT:")) {
                    outDegree += Integer.parseInt(s.substring(4));
                } else if (s.equals("IN")) {
                    // From mapper directly (no combiner)
                    inDegree++;
                } else if (s.equals("OUT")) {
                    outDegree++;
                }
            }

            context.write(key, new Text("in=" + inDegree + " out=" + outDegree));
        }
    }

    // -------------------------------------------------------------------------
    // STEP 5: Main - wire everything together
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {

        // If called with "generate" arg, just generate the edge list
        if (args.length == 1 && args[0].equals("generate")) {
            generateEdgeList("edges.txt", 1000, 5000);
            return;
        }

        if (args.length < 2) {
            System.err.println("Usage: Task1b_DegreeCounting <input_path> <output_path> [--no-combiner]");
            System.exit(1);
        }

        String inputPath  = args[0];
        String outputPath = args[1];
        boolean useCombiner = !(args.length == 3 && args[2].equals("--no-combiner"));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Degree Counting - combiner=" + useCombiner);

        job.setJarByClass(Task1b_DegreeCounting.class);

        // Set Mapper and Reducer
        job.setMapperClass(DegreeMapper.class);
        job.setReducerClass(DegreeReducer.class);

        // Optionally set Combiner
        if (useCombiner) {
            job.setCombinerClass(DegreeCombiner.class);
            System.out.println(">> Running WITH combiner");
        } else {
            System.out.println(">> Running WITHOUT combiner");
        }

        // Output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Time the job
        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        System.out.println("Job finished in: " + (endTime - startTime) + " ms");
        System.exit(success ? 0 : 1);
    }
}
