import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import net.minidev.json.reader.ArrayWriter;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, ArrayWritable> {
    private Text word = new Text();

    public void map(Object key,
                    Text value,
                    Context context
    ) throws IOException,InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        String nextString = itr.nextToken();
        nextString = nextString.replaceAll("[^(\\x20|(\\x41-\\x5A)|(\\x61-\\x7A))]", "");
        nextString = nextString.toLowerCase();
        word.set(nextString);
        String[] temp = {"0", context.getCurrentKey().toString(), "0"};
        context.write(word, new ArrayWritable(temp));
      }
    }
  }

  public static class SortShuffler extends Reducer<Text, ArrayWritable, Text, ArrayWritable> {

    private ArrayWritable result;

    public void reduce(Text key, Iterable<ArrayWritable> values, Context context)
            throws IOException, InterruptedException {

      // Get the context info about this key
      String[] info = (String[]) context.getCurrentValue().toArray();
      // Sum of all occurrence of `word'
      int total_sum = Integer.parseInt(info[0]);

      // Array of `word' occurrence per file
      ArrayList<String[]> file_tuples = new ArrayList<>();

      // For values in writable get array per file and increase value counter if it is in same
      // file that is currently processing
      for (ArrayWritable val: values) {
        int sum = Integer.parseInt(info[2]);
        if (info[1].equals(((String[]) val.toArray())[1])){
          ++sum;
        }
        String[] holder = {info[1], Integer.toString(sum)};
        file_tuples.add(holder);
        ++total_sum;
      }

      String[] tot = {Integer.toString(total_sum)};
      file_tuples.add(0, tot);

      String[] temp = (String[]) file_tuples.toArray();
      result = new ArrayWritable(temp);
      // May throws errors if it treats zero as string and not doing internal cast to int
      context.write(key, result);
    }
  }


  public static class IntSumReducer extends Reducer<Text, ArrayWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();


    public void reduce(Text key, Iterable<ArrayWritable> values, Context context)
            throws IOException, InterruptedException {
      int tfidf = 0;
      int total = Integer.parseInt(values.iterator().next().get()[0].toString());
      for (ArrayWritable val : values) {
        tfidf += 1;
      }
      result.set((double)total / (double) (tfidf-1));
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SortShuffler.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ArrayWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}