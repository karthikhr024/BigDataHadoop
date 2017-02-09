package kmeansDemo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author cloudera
 *
 */
public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Double x[] = new Double[10];
        
        for (int i = 0; i < 10; i++) {
        	 x[i]= new Double(0);
		}
        
        int count = 0;
        for (Text value: values) {
            String[] strArray = value.toString().split(",");
            
            for (int i = 0; i < 10; i++) {
            	x[i] = x[i] + Double.parseDouble(strArray[i]);
			}
            count ++;
        }

        for(int i = 0; i < 10; i++){
        	x[i] = x[i]/count;
        }
        
        String centroidAsString = x[0] + "," + x[1] + "," + x[2] + "," + x[3] + "," + x[4] + "," + x[5] + "," + x[6] + "," + x[7] + "," + x[8] + "," + x[9];

        context.write(new Text(""+key), new Text(centroidAsString));
    }

}