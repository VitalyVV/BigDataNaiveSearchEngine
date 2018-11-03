package indexing;

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


public class Merge {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String initial = value.toString().trim();
      String splitted[] = initial.split("\\s+");
      String msg;
      if (splitted.length > 1) {

        StringBuilder b = new StringBuilder();
        for (String s: splitted)
          b.append(s + "#");
        msg = b.toString();

        StringTokenizer itr = new StringTokenizer(initial);
        while (itr.hasMoreTokens()) {
          String identificator = itr.nextToken();
          if (identificator.contains(".")) {
            //read file
            String[] split = itr.nextToken().split("#");
            String word = split[0];
            String amount = split[1];
//          System.out.println("mapper writes " + word + ":" + amount
//              + "#" + identificator);
            context.write(new Text(word),
                new Text(amount + "#" + identificator));
          } else {
            //read word
            String amount;
            try {
              amount = itr.nextToken();
            } catch (RuntimeException e) {
              throw new CustomException("chert", e, String.format("Reason = [%s], content=[%s]", initial, msg));
            }
//					System.out.println(
//							"mapper writes " + identificator + ":" + amount);
            context.write(new Text(identificator), new Text(amount));
          }
          // format: word amount(#fileName)
        }
      }
    }
  }

  /**
   * // reduce to file public static class FileReducer extends Reducer<Text, Text, Text, Text> {
   *
   * public void reduce( Text key, Iterable<Text> values, Context context) throws IOException,
   * InterruptedException { int globalCount = -1; LinkedList<String> writeList = new
   * LinkedList<String>(); for (Text val : values) { if (!val.toString().contains("#")) {
   * globalCount = Integer.parseInt(val.toString()); } else { writeList.add(val); } }
   *
   * System.out.println("global count"+globalCount+Integer.toString(writeList.size())+"Key"+key.toString()+".");
   * for(Text val:writeList) { System.out.println("writeList"+val.toString()); } for (Text val :
   * writeList) { String[] split = val.toString().split("#"); int amount =
   * Integer.parseInt(split[0]); String fileName = split[1]; context.write(new Text(fileName), new
   * Text(Integer.toString(amount / globalCount))); System.out.println("file reducer" + fileName +
   * ":" + Integer.toString(amount / globalCount)); // writes the filename with TF/IDF
   *
   * } } }
   */


  public static class PrintReducer
      extends
      Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int globalCount = -1;
      LinkedList<String> writeList = new LinkedList<String>();
      for (Text val : values) {
        System.out.println("val in iterator" + val.toString());
        if (!val.toString().contains("#")) {
          globalCount = Integer.parseInt(val.toString());
        } else {
          writeList.add(val.toString());
        }
      }

      System.out.println(
          "global count" + globalCount + " " + Integer.toString(writeList.size()) + "Key" + key
              .toString() + ".");
      for (String val : writeList) {
	  if(!val.contains("#")) {
	      System.out.println("problem"+val);
	  }
        String[] split = val.split("#");
        float amount = Integer.parseInt(split[0]);
        String fileName = split[1];
        context.write(new Text(fileName),
            new Text(key.toString() + "#" + Float.toString(amount / globalCount)));
//        System.out.println("file reducer" + fileName + ":"
//            + Float.toString(amount / globalCount));
        // writes the filename with TF/IDF

      }
      /**
       // prints one line per file : fileName word#TF/IDF#word#TF/IDF....
       StringBuilder builder = new StringBuilder();
       for (Text val : values)
       {
       builder.append(val.toString());
       builder.append("#");
       }
       System.out.println(
       "writes as output " + key.toString() + builder.toString());
       context.write(key, new Text(builder.toString()));

       */
    }

  }


  public static void main(
      String[] args)
      throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "merge files");
    job.setJarByClass(Merge.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(FileReducer.class);
    job.setReducerClass(PrintReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
