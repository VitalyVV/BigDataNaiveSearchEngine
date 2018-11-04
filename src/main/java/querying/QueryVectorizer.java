package querying;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * Converts query with vocabulary into queue.
 */

public class QueryVectorizer {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
      Text fileName = new Text(
          ((FileSplit) context.getInputSplit()).getPath().getName());

      System.out.println(fileName.toString());
      if (fileName.toString().contains("queryInput")) {
        while (itr.hasMoreTokens()) {
          String word = itr.nextToken();
          // is query file
          String nextString = word.replaceAll("[^a-zA-Z]", "").toLowerCase();
          if (map.containsKey(nextString)) {
            map.put(nextString, map.get(nextString) + 1);
          } else {
            map.put(nextString, 1);
          }
        }
        for (Map.Entry<String, Integer> val : map.entrySet()) {
          if (!val.getKey().equals("")) {
            context.write(new Text(val.getKey()), new Text(Integer.toString(val.getValue())));
          }
        }
      } else {
        while (itr.hasMoreTokens()) {
          String word = itr.nextToken();
          String amount = itr.nextToken();
          context.write(new Text(word), new Text(amount + "#"));
        }
      }
      // format: word amount(#) , appends(#) if IDF count
    }
  }

  // reduce to file
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      // HashList: Word:Count
      StringBuilder builder = new StringBuilder();
      float globalCount = -1;
      float queryCount = 0;
      for (Text val : values) {
        if (val.toString().contains("#")) {
          // idf
          globalCount = Integer.parseInt(val.toString().replace("#", ""));
        } else {
          // count query
          queryCount = Integer.parseInt(val.toString());
        }
      }
      if (queryCount != 0) {
        context.write(key, new Text(Float.toString(queryCount / globalCount)));
      }

      // concats all the words in file : fileName word#TF/IDF#word#TF/IDF...
    }
  }
}
