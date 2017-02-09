package pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;


public class Node {
  private double pageRank = 0.25;
  private String[] adjacentNodeNames;

  public static final char fieldSeparator = ',';

  public double getPageRank() {
    return pageRank;
  }

  public Node setPageRank(double pageRank) {
    this.pageRank = pageRank;
    return this;
  }

  public String[] getAdjacentNodes() {
    return adjacentNodeNames;
  }

  public Node setAdjacentNodes(String[] adjacentNodeNames) {
    this.adjacentNodeNames = adjacentNodeNames;
    return this;
  }

  public boolean hasAdjacentNodes() {
    return adjacentNodeNames != null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(pageRank);

    if (getAdjacentNodes() != null) {
      sb.append(fieldSeparator)
          .append(StringUtils
              .join(getAdjacentNodes(), fieldSeparator));
    }
    return sb.toString();
  }

  public static Node getNodefromMROutputString(String value) throws IOException {
    String[] parts = StringUtils.splitPreserveAllTokens(
        value, fieldSeparator);
    if (parts.length < 1) {
      throw new IOException(
          "Expected 1 or more parts but received " + parts.length);
    }
    Node node = new Node()
        .setPageRank(Double.valueOf(parts[0]));
    if (parts.length > 1) {
      node.setAdjacentNodes(Arrays.copyOfRange(parts, 1,
          parts.length));
    }
    return node;
  }
}