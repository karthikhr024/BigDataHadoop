package charcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class CharcountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		
		String line = value.toString();
		for (String word : line.split(" ")) {
			Character fisrtAlphabetFromWord = getFisrtAlphabetFromWord(word.toLowerCase());
			if (fisrtAlphabetFromWord!=null) {
				output.collect(new Text(""+ fisrtAlphabetFromWord), new IntWritable(1));
			}
		}
		
	}
	
	private Character getFisrtAlphabetFromWord(String word){
		char[] charArray = word.toCharArray();
		for (int i = 0; i < charArray.length; i++) {
			if(Character.isAlphabetic(charArray[i])){
				return charArray[i];
			}
		}
		return null;
	}
}
