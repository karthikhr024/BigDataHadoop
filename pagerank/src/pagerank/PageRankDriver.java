package pagerank;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class PageRankDriver {

	public static void main(String... args) throws Exception {
		String inputFile = args[0];
		String outputDir = args[1];
		mapReduceLoop(inputFile, outputDir);
	}

	public static void mapReduceLoop(String input, String output)
			throws Exception {

		Configuration conf = new Configuration();
		Path outputPath = new Path(output);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		outputPath.getFileSystem(conf).mkdirs(outputPath);
		Path inputPath = new Path(outputPath, "input.txt");
		int numNodes = createInputFile(new Path(input), inputPath);

		int jobNum = 1;
		while (jobNum <= 10) {

			Path jobOutputPath = new Path(outputPath, String.valueOf(jobNum));
			launchJob(inputPath, jobOutputPath, numNodes);
			inputPath = jobOutputPath;
			jobNum++;
		}
		writeFinalOutput(inputPath);
	}

	public static int createInputFile(Path file, Path targetFile)
			throws IOException {
		Configuration conf = new Configuration();

		FileSystem fs = file.getFileSystem(conf);

		int numNodes = getNumNodes(file);
		double initialPageRank = 1.0 / (double) numNodes;
		OutputStream os = fs.create(targetFile);
		LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");

		while (iter.hasNext()) {
			String line = iter.nextLine();

			String[] parts = StringUtils.split(line);

			Node node = new Node().setPageRank(initialPageRank)
					.setAdjacentNodes(
							Arrays.copyOfRange(parts, 1, parts.length));
			IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
		}
		os.close();
		return numNodes;
	}

	public static int getNumNodes(Path file) throws IOException {
		Configuration conf = new Configuration();

		FileSystem fs = file.getFileSystem(conf);

		return IOUtils.readLines(fs.open(file), "UTF8").size();
	}

	public static void launchJob(Path inputPath, Path outputPath, int numNodes)
			throws Exception {
		Configuration conf = new Configuration();
		conf.setInt(PageRankReducer.CONF_NUM_NODES_GRAPH, numNodes);

		Job job = Job.getInstance(conf, "PageRankJob");
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(true)) {
			throw new Exception("Job failed");
		}

	}

	private static void writeFinalOutput(Path jobOutputPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = jobOutputPath.getFileSystem(conf);
		Path finalOutputFilePath = new Path(jobOutputPath, "final-output.txt");
		OutputStream os = fs.create(finalOutputFilePath);
		LineIterator iter = IOUtils.lineIterator(fs.open(new Path(jobOutputPath, "part-r-00000")), "UTF8");

		while (iter.hasNext()) {
			String line = iter.nextLine();
			String[] parts = StringUtils.split(line, "\t");
			IOUtils.write("node"+parts[0] + '\t' + parts[1].split(",")[0] + '\n', os);
		}
		os.close();
		
		
	}

}
