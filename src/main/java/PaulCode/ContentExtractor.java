import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Converts query with vocabulary into queue.
 */

public class ContentExtractor {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
		    String word = itr.nextToken();
		    String count = itr.nextToken();
		    context.write(new Text("None"), new Text(word+"#"+count));//dummy key 
		}
	    }
	    // format: word amount#query/FileName
	}


    // reduce to file
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    // HashList: Word:Count
	    
	    List<String> sortedList = new ArrayList<>();
	    for (Text val : values) {
	        sortedList.add(val.toString());
	    }
	    Collections.sort(sortedList);
	    
	    float sum = 0;
	    float maxSum = -1;
	    String maxFile = "";
	    String previousFile = "";
	    for (String val : sortedList) {
		String [] split = val.split("#");
		if(previousFile.equals(split[0])) {
		    sum += Float.valueOf(split[1]); 
		    if(sum > maxSum) {
			maxSum = sum;
			maxFile = split[0];
		    }
		}
		else {
		    previousFile = split[0];
		    sum = Float.valueOf(split[1]);
		}
		
	    }
	    context.write(new Text(maxFile),new Text(String.valueOf(maxSum)));
	    //prints the files with their values 
	}

    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "file count");
	job.setJarByClass(ContentExtractor.class);
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
