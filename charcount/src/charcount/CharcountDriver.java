package charcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CharcountDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {

		if(args.length<2)
			return -1;
		JobConf jobConf = new JobConf(CharcountDriver.class);
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		jobConf.setMapperClass(CharcountMapper.class);
		jobConf.setReducerClass(CharcountReducer.class);
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(IntWritable.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(jobConf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CharcountDriver(), args);
		System.out.println(exitCode);
	}
	
}
