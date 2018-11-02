package subtasks;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Converts query with vocabulary into queue.
 */

public class MaxFinder {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String word = itr.nextToken();
        String count = itr.nextToken();
        context.write(new Text("None"), new Text(count + "#" + word));// dummy key to compare
      }
    }
    // format: word amount#query/FileName
  }


  // reduce to file
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // HashList: Word:Count
      float max = -1;
      String maxName = "";
      for (Text val : values) {
        String[] split = val.toString().split("#");
        System.out.println("works" + split[0] + split[1]);
        if (Float.parseFloat(split[0]) > max) {
          max = Float.parseFloat(split[0]);
          maxName = split[1];
        }
      }
      context.write(new Text(maxName), new Text(""));// dummy print for name
      //prints the maximum fileName
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "file count");
    job.setJarByClass(MaxFinder.class);
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
