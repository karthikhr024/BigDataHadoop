package joinSimulation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InnerJoinReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, IntWritable> {

	@Override
	public void reduce(IntWritable key, Iterator<Text> value,
			OutputCollector<IntWritable, IntWritable> outputCollector,
			Reporter reporter) throws IOException {

		ArrayList<String> pageViewTableTuples = new ArrayList<String>();
		ArrayList<String> usersTableTuples = new ArrayList<String>();
		while (value.hasNext()) {
			String tuple = value.next().toString();
			if (tuple.startsWith("1")) {
				pageViewTableTuples.add(tuple);
			} else if (tuple.startsWith("2")) {
				usersTableTuples.add(tuple);
			}
		}

		for (String pageTuple : pageViewTableTuples) {
			for (String userTuple : usersTableTuples) {
				outputCollector.collect(
						new IntWritable(
								Integer.parseInt(pageTuple.split("\t")[1])),
						new IntWritable(
								Integer.parseInt(userTuple.split("\t")[2])));
			}
		}

	}

}
