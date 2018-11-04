package querying;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Converts query with vocabulary into queue.
 */

public class Relevance {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      Path path = ((FileSplit) context.getInputSplit()).getPath();

      if (path.toString().contains("vectorized")) {  // TODO: not obvious
        // query file
        while (itr.hasMoreTokens()) {
          String word = itr.nextToken();
          String count = itr.nextToken();
          context.write(new Text(word), new Text(count + "#" + "query"));
        }
      } else {
        while (itr.hasMoreTokens()) {
          String name = itr.nextToken();
          String amount = itr.nextToken();
          String[] split = amount.split("#");
          for (int i = 0; i < (split.length - 1); i++) {
            context.write(new Text(split[i]), new Text(split[i + 1] + "#" + name));
//            System.out.println("mapper files write" + split[i] + " " + split[i + 1] + '#' + name);
            i = i + 1;
          }
        }
      }
      // format: word amount#query/FileName
    }
  }

  // reduce to file
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // HashList: Word:Count
      LinkedList<String> files = new LinkedList<String>();
      float queryCount = 0;
      for (Text val : values) {
        if (val.toString().contains("query")) {
          // query count
          queryCount = Float.valueOf(val.toString().replace("#query", ""));
        } else {
          // count query
          files.add(val.toString());
        }
      }

      if (queryCount != 0) {
        for (String val : files) {
          String[] split = val.split("#");
          float mult = queryCount * Float.valueOf(split[0]);
          context.write(new Text(split[1]), new Text(Float.toString(mult)));
        }
      }
      // concats all the words in file : fileName word#TF/IDF#word#TF/IDF...
    }
  }
}
