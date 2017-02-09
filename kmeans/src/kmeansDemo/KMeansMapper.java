package kmeansDemo;



import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author cloudera
 *
 */
public class KMeansMapper extends Mapper<Object, Text, Text	, Text> {

    public static List<Double[]> centroids;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        centroids = Utilities.readCentroidsFromFile(cacheFiles[0].toString());
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] record = value.toString().split("\t");
        Double[] dataPoints = Utilities.getFormattedDataPoints(record[1]); 
        int index = 0;
        double minDistance = Double.MAX_VALUE;
        for (int j = 0; j < centroids.size(); j++) {
        	double distance = Utilities.euclideanDistanceMeasure(centroids.get(j), dataPoints);
            if (distance < minDistance) {
                index = j;
                minDistance = distance;
            }
        }
        context.write(new Text("C"+index), new Text(record[1].toString()));
    }
}