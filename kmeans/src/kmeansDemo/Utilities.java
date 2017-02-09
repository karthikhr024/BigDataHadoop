package kmeansDemo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
	
/**
 * 
 * @author cloudera
 *
 */
public class Utilities {
	
    public static List<Double[]> readCentroidsFromFile(String filename) throws IOException {
        FileInputStream fis = new FileInputStream(filename);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        return  readData(br);
    }

    public static List<Double[]> getCentroids(String fileContent) throws IOException {
        BufferedReader br = new BufferedReader(new StringReader(fileContent));
        return  readData(br);
    }

    private static List<Double[]> readData(BufferedReader br) throws IOException {
        List<Double[]> centroids = new ArrayList<>();
        String line;
        try {
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\t");
                String[] temp = values[1].split(",");
                Double[] centroid = new Double[10];
                
                for (int i = 0; i < 10; i++) {
                	centroid[i] = Double.parseDouble(temp[i]);
				}
                centroids.add(centroid);
            }
        }
        finally {
            br.close();
        }
        return centroids;
    }

    public static String getFormattedCentroids(List<Double[]> centroids) {

        int count = 0;
        StringBuilder centroidsStringBuilder = new StringBuilder();
        for (Double[] centroid : centroids) {
        	centroidsStringBuilder.append("C" + count++);
        	centroidsStringBuilder.append("\t");
        	for (int i = 0; i < 9; i++) {
        		 centroidsStringBuilder.append(centroid[0].toString());
                 centroidsStringBuilder.append(",");
			}
            centroidsStringBuilder.append(centroid[9].toString());
            centroidsStringBuilder.append("\n");
        }

        return centroidsStringBuilder.toString();
    }
    
    public static Double[] getFormattedDataPoints(String points) {
    	String[] split = points.split(",");
    	Double[] formattedDataPoints = new Double[split.length];
    	for (int i = 0; i < split.length; i++) {
    		formattedDataPoints[i] = Double.parseDouble(split[i]);	
		}
    	return formattedDataPoints;
	}
    
    

    public static void writeCentroidsToFile(Configuration config, String formattedCentroids) throws IOException {

        FileSystem fs = FileSystem.get(config);
        FSDataOutputStream fin = fs.create(new Path(Constants.CENTROIDS_FILE));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
        bw.append(formattedCentroids);
        bw.close();
    }

    public static List<Double[]> createRandomCentroids(int centroidsSize) {
        List<Double[]> centroidsList = new ArrayList<>();

        // computes randomly centroids
        for (int j = 0; j < centroidsSize; j++) {
            Double[] centroid = new Double[10];
            for (int i = 0; i < 10; i++) {
            	 centroid[i] = Math.random() * 2;
			}
            centroidsList.add(centroid);
        }

        return centroidsList;
    }


    public static double euclideanDistanceMeasure(Double[] centroids, Double[] dataPoints){
    	
    	return Math.sqrt(Math.pow(dataPoints[0] - centroids[0], 2) + Math.pow(dataPoints[1] - centroids[1], 2)
    			+ Math.pow(dataPoints[2] - centroids[2], 2) + Math.pow(dataPoints[3] - centroids[3], 2)
    			+ Math.pow(dataPoints[4] - centroids[4], 2) + Math.pow(dataPoints[5] - centroids[5], 2)
    			+ Math.pow(dataPoints[6] - centroids[6], 2) + Math.pow(dataPoints[7] - centroids[7], 2)
    			+ Math.pow(dataPoints[8] - centroids[8], 2) + Math.pow(dataPoints[9] - centroids[9], 2));
    }

    public static String readReducerOutput(Configuration config) throws IOException {
        FileSystem fs = FileSystem.get(config);
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(config.get(Constants.OUTPUT_FILE_ARG) + "/part-r-00000")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line).append("\n");
        }
        reader.close();
        return content.toString();
    }

	
}