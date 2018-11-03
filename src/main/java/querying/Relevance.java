package querying;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Converts query with vocabulary into queue.
 */

public class Relevance {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
      Text fileName = new Text(((FileSplit) context.getInputSplit()).getPath().getName());

      if (fileName.toString().contains("query")) {
        LinkedList<String> list = new LinkedList<>();
        // query file
        while (itr.hasMoreTokens()) {
          String word = itr.nextToken();
          String count = itr.nextToken();
          context.write(new Text(word), new Text(count + "#" + "query"));
//          System.out.println("query reads" + word + count);
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
//          System.out.println(
//              "processed word" + split[1] + " " + (Float.toString(mult) + "word" + split[0]));
        }
      }

      // concats all the words in file : fileName word#TF/IDF#word#TF/IDF...
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "file count");
    job.setJarByClass(Relevance.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
