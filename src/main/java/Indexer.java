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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Collections;
import subtasks.FileCount;
import subtasks.Merge;
import subtasks.OccurCount;

/**
 * Converts to machine easily readible file
 */


public class Indexer {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
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
//    Job job1 = Job.getInstance(conf, "File Count");
//    job1.setJarByClass(FileCount.class);
//    job1.setMapperClass(FileCount.TokenizerMapper.class);
//    job1.setReducerClass(FileCount.IntSumReducer.class);
//    job1.setOutputKeyClass(Text.class);
//    job1.setOutputValueClass(Text.class);
//
//    FileInputFormat.addInputPath(job1, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job1, new Path("wcoutput_1/"));
//    if (!job1.waitForCompletion(true)) {
//      System.exit(1);
//    }
//
//
//    Job job2 = Job.getInstance(conf, "Occur Count");
//    job2.setJarByClass(OccurCount.class);
//    job2.setMapperClass(OccurCount.TokenizerMapper.class);
//    job2.setReducerClass(OccurCount.IntSumReducer.class);
//    job2.setOutputKeyClass(Text.class);
//    job2.setOutputValueClass(Text.class);
//
//
//    FileInputFormat.addInputPath(job2, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job2, new Path("wcoutput_2/"));
//    if (!job2.waitForCompletion(true)) {
//      System.exit(1);
//    }

    Job job3 = Job.getInstance(conf, "merge task");
    job3.setJarByClass(Merge.class);
    job3.setMapperClass(Merge.TokenizerMapper.class);
    job3.setReducerClass(Merge.PrintReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);

//    MultipleInputs.addInputPath(job3, new Path("wcoutput_1/"), TextInputFormat.class, Merge.TokenizerMapper.class);
//    MultipleInputs.addInputPath(job3, new Path("wcoutput_2/"), TextInputFormat.class, Merge.TokenizerMapper.class);
    FileInputFormat.setInputPaths(job3, new Path("wcoutput_1"), new Path("wcoutput_2"));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));

    if (!job3.waitForCompletion(true))
      System.exit(1);

//
//    Job job4 = Job.getInstance(conf, "Indexer job");
//    job4.setJarByClass(Indexer.class);
//    job4.setMapperClass(Indexer.TokenizerMapper.class);
//    //job.setCombinerClass(IntSumReducer.class);
//    job4.setReducerClass(Indexer.IntSumReducer.class);
//
//    job4.setOutputKeyClass(Text.class);
//    job4.setOutputValueClass(Text.class);
//    FileInputFormat.addInputPath(job4, new Path("wcoutput_2/"));
//    FileOutputFormat.setOutputPath(job4, new Path(args[1]));
//    System.exit(job4.waitForCompletion(true) ? 0 : 1);
  }
}
