import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Text;

import java.io.IOException;
import java.util.ArrayList;
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

public class QueryVectorizer
{

	public static class TokenizerMapper
			extends
			Mapper<Object, Text, Text, Text>
	{

		private Text word = new Text();


		public void map(
				Object key,
				Text value,
				Context context)
				throws IOException,
				InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			StringBuilder builder = new StringBuilder();
			LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
			Text fileName = new Text(
					((FileSplit) context.getInputSplit()).getPath().getName());

			System.out.println(fileName.toString());
			if (fileName.toString().contains("queryInput"))
			{
				while (itr.hasMoreTokens())
				{
					String word = itr.nextToken();
					System.out.println("in loop");
					// is query file
					String nextString = word.replaceAll(
							"[^(\\x20|(\\x41-\\x5A)|(\\x61-\\x7A))]", "");
					nextString = nextString.toLowerCase();
					if (map.containsKey(nextString))
					{
						map.put(nextString, map.get(nextString) + 1);
					}
					else
					{
						map.put(nextString, 1);
					}
				}
				for (Map.Entry<String, Integer> val : map.entrySet())
				{
					context.write(new Text(val.getKey()),
							new Text(Integer.toString(val.getValue())));
					System.out.println("");
					System.out.println("");
					System.out.println("");
					System.out.println("query" + val.getKey()
							+ Integer.toString(val.getValue()));
				}
			}
			else
			{
				while (itr.hasMoreTokens())
				{
					String word = itr.nextToken();
					String amount = itr.nextToken();
					System.out.println("mapper writes " + word + ":" + amount);
					context.write(new Text(word), new Text(amount + "#"));
				}
			}
			// format: word amount(#) , appends(#) if IDF count
		}

	}


	// reduce to file 
	public static class IntSumReducer
			extends
			Reducer<Text, Text, Text, Text>
	{

		public void reduce(
				Text key,
				Iterable<Text> values,
				Context context)
				throws IOException,
				InterruptedException
		{
			// HashList: Word:Count
			StringBuilder builder = new StringBuilder();
			float globalCount = -1;
			float queryCount = 0;
			for (Text val : values)
			{
				if (val.toString().contains("#"))
				{
					// idf
					globalCount = Integer
							.parseInt(val.toString().replace("#", ""));
				}
				else
				{
					// count query
					queryCount = Integer.parseInt(val.toString());
				}
			}
			StringBuilder builder = new StringBuilder();
			if (queryCount != 0)
			{
				System.out.println(key.toString()+" "+globalCount+" "+queryCount);
				context.write(key,
						new Text(Float.toString(queryCount / globalCount)));
			}

			// concats all the words in file : fileName word#TF/IDF#word#TF/IDF...
		}

	}


	public static void main(
			String[] args)
			throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "file count");
		job.setJarByClass(QueryVectorizer.class);
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
