package PaulCode;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
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
 * Counts occurences of word in files. IDF
 */
public class OccurCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private final Gson g = new Gson();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      DatasetDescription dd =  g.fromJson(value.toString(), DatasetDescription.class);


      StringTokenizer itr = new StringTokenizer(String.format("%s %s", dd.title, dd.text));
      while (itr.hasMoreTokens()) {
        String nextString = itr.nextToken();
        nextString = nextString.replaceAll("[^(\\x20|(\\x41-\\x5A)|(\\x61-\\x7A))]", "");
        nextString = nextString.toLowerCase();
        word.set(nextString);
        Text fileName = new Text(((FileSplit) context.getInputSplit())
            .getPath().getName());
        System.out.println(fileName.toString() + " " + word.toString());
        context.write(word, fileName);

        // maps word fileName
      }
    }
  }


  // reduce to file
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // HashList: Word:Count
      LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
      LinkedList<String> memorizeList = new LinkedList<String>();
      for (Text val : values) {
        System.out.println("read text" + val.toString());
        if (map.containsKey(key.toString())) {
          if (!memorizeList.contains(val.toString())) {
            // count word per file only once
            map.put(key.toString(), map.get(key.toString()) + 1);
            memorizeList.add(val.toString());
            System.out.println("puttet higher" + key.toString());
          }
        } else {
          map.put(key.toString(), 1);
          memorizeList.add(val.toString());
          System.out.println("new entry" + key.toString());
        }

      }

      for (Map.Entry<String, Integer> value : map.entrySet()) {
        context.write(new Text(value.getKey()), new Text(Integer.toString(value.getValue())));
        System.out.println(value.getKey() + Integer.toString(value.getValue()));
      }

    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "file count");
    job.setJarByClass(OccurCount.class);
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
