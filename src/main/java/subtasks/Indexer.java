package subtasks;

import java.io.IOException;
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

import java.util.Collections;

/**
 * Converts to machine easily readible file
 */


public class Indexer {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String identificator = itr.nextToken();
        String wordAmount = itr.nextToken();
        context.write(new Text(identificator), new Text(wordAmount));

        // maps FileName word#amount
      }
    }
  }


  // reduce to file
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      LinkedList<String> list = new LinkedList<String>();
      for (Text val : values) {
        list.add(val.toString());
      }
      Collections.sort(list);
      StringBuilder builder = new StringBuilder();

      for (String val : list) {
        builder.append(val);
        builder.append("#");
      }
      context.write(key, new Text(builder.toString()));

      // concats all the words in file and sorts alphanumerically: fileName word#TF/IDF#word#TF/IDF...
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "file count");
    job.setJarByClass(Indexer.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
