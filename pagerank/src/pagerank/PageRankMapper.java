package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		context.write(key, value);
		Node node = Node.getNodefromMROutputString(value.toString());
		if (node.getAdjacentNodes() != null
				&& node.getAdjacentNodes().length > 0) {
			double outboundPageRank = node.getPageRank()
					/ (double) node.getAdjacentNodes().length;
			for (int i = 0; i < node.getAdjacentNodes().length; i++) {
				String neighbor = node.getAdjacentNodes()[i];
				outKey.set(neighbor);
				Node adjacentNode = new Node().setPageRank(outboundPageRank);
				outValue.set(adjacentNode.toString());
				context.write(outKey, outValue);
			}
		}
	}
}