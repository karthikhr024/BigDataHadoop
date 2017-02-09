package joinSimulation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InnerJoinMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> outputCollector,
			Reporter reporter) throws IOException {

		FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		String tuples = value.toString();
		if (!isHeader(tuples)) {
			if (fileName.equals("page_view.txt")) {
				tuples = "1\t" + tuples;
				outputCollector
						.collect(
								new IntWritable(Integer.parseInt(tuples
										.split("\t")[2])), new Text(tuples));
			} else if (fileName.equals("user.txt")) {
				tuples = "2\t" + tuples;
				outputCollector
						.collect(
								new IntWritable(Integer.parseInt(tuples
										.split("\t")[1])), new Text(tuples));
			}
		}

	}

	private boolean isHeader(String tuples) {
		if (tuples.split("\t")[0].equals("page_id")
				|| tuples.split("\t")[0].equals("user_id")) {
			return true;
		}
		return false;
	}

}
