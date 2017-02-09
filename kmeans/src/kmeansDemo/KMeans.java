package kmeansDemo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @author cloudera
 *
 */
public class KMeans {

	public static void main(String[] args) throws Exception {

		System.out.println("args.length" + args.length);
		Configuration config = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: KMeans <in> <out>");
			System.exit(2);
		}

		int centroidsNumber = 4;
		config.setInt(Constants.CENTROID_NUMBER_ARG, centroidsNumber);
		config.set(Constants.INPUT_FILE_ARG, args[0]);

		List<Double[]> centroids = Utilities.createRandomCentroids(centroidsNumber);
		System.out.println("centroids :: " + centroids);
		String centroidsFile = Utilities.getFormattedCentroids(centroids);
		System.out.println("centroidsFile::" + centroidsFile);
		// writes intial centroids on distributed cache
		Utilities.writeCentroidsToFile(config, centroidsFile);
		int iteration = 0;
		do {

			config.set(Constants.OUTPUT_FILE_ARG, args[1] + "-"
					+ iteration);

			// executes hadoop job
			if (!launchJob(config)) {
				// if an error has occurred stops iteration and terminates
				System.exit(1);
			}

			// reads reducer output file
			String newCentroids = Utilities.readReducerOutput(config);

			// writes the reducers output to distributed cache
			Utilities.writeCentroidsToFile(config, newCentroids);

			centroidsFile = newCentroids;
			iteration++;

		} while (iteration < 10);

		// now that we have computed the centroids, write to file
		writeFinalData(config, Utilities.getCentroids(centroidsFile));
	}

	/**
	 * executes the job
	 *
	 * @return true if the job has converged, else false
	 */
	private static boolean launchJob(Configuration configuration)
			throws Exception {

		Job job = Job.getInstance(configuration);
		job.setJobName("KMeans");
		job.setJarByClass(KMeans.class);

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		job.addCacheFile(new Path(Constants.CENTROIDS_FILE).toUri());

		FileInputFormat.addInputPath(job,
				new Path(configuration.get(Constants.INPUT_FILE_ARG)));
		FileOutputFormat.setOutputPath(job,
				new Path(configuration.get(Constants.OUTPUT_FILE_ARG)));

		return job.waitForCompletion(true);
	}

	public static void writeFinalData(Configuration configuration,
			List<Double[]> centroids) throws IOException {

		FileSystem fs = FileSystem.get(configuration);

		FSDataOutputStream dataOutputStream = fs.create(new Path(configuration
				.get(Constants.OUTPUT_FILE_ARG) + "/final-data"));
		FSDataInputStream dataInputStream = new FSDataInputStream(
				fs.open(new Path(configuration.get(Constants.INPUT_FILE_ARG))));

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				dataInputStream));
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				dataOutputStream));

		try {

			String line;
			Map<String, ArrayList<String>> finalOutputMap = new HashMap<String, ArrayList<String>>();
			finalOutputMap.put("cluster1", new ArrayList<String>());
			finalOutputMap.put("cluster2", new ArrayList<String>());
			finalOutputMap.put("cluster3", new ArrayList<String>());
			finalOutputMap.put("cluster4", new ArrayList<String>());
			while ((line = reader.readLine()) != null) {

				String[] values = line.split("\t");
				int id = Integer.parseInt(values[0]);
				Double[] dataPoints = Utilities.getFormattedDataPoints(values[1]);
				int index = 0;
				double minDistance = Double.MAX_VALUE;
				for (int j = 0; j < centroids.size(); j++) {
					double distance = Utilities.euclideanDistanceMeasure(centroids.get(j),
							dataPoints);
					if (distance < minDistance) {
						index = j;
						minDistance = distance;
					}
				}
				finalOutputMap.get("cluster"+(index+1)).add("id"+id);
			}
			
			for (Map.Entry<String, ArrayList<String>> entry : finalOutputMap.entrySet()) {
					ArrayList<String> ids = entry.getValue();
					StringBuilder idsString = new StringBuilder();
					for (int i = 0; i < ids.size(); i++) {
						idsString.append(ids.get(i));
						
						if(i!=ids.size()-1){
							idsString.append(",");
						}else{
							idsString.append("\n");
						}
					}
					writer.write(entry.getKey() + "\t" + idsString);
			}
			
		} finally {

			if (reader != null) {
				reader.close();
			}
			if (writer != null) {
				writer.close();
			}
		}
	}

}