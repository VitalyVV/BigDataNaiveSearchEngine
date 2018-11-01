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
 * Counts occurs of words per file.
 */


public class FileCount
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
			while (itr.hasMoreTokens())
			{
				String nextString = itr.nextToken();
				nextString = nextString.replaceAll(
						"[^(\\x20|(\\x41-\\x5A)|(\\x61-\\x7A))]", "");
				nextString = nextString.toLowerCase();
				word.set(nextString);
				Text fileName = new Text(((FileSplit) context.getInputSplit())
						.getPath().getName());
				System.out.println(fileName.toString() + " " + word.toString());
				context.write(fileName, word);

				// maps FileName word
			}
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
			LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
			LinkedList<String> memorizeList = new LinkedList<String>();
			for (Text val : values)
			{
				System.out.println("read text" + val.toString());
				if (map.containsKey(val.toString()))
				{
					// count word per file only once
						map.put(val.toString(), map.get(val.toString()) + 1);
						memorizeList.add(key.toString());
						System.out.println("puttet higher" + val.toString());
				}
				else
				{
					map.put(val.toString(), 1);
					memorizeList.add(key.toString());
					System.out.println("new entry" + val.toString());
				}

				for (Map.Entry<String, Integer> value : map.entrySet())
				{
					System.out.println("print map" + value.getKey()
							+ Integer.toString(value.getValue()));
				}
			}

			for (Map.Entry<String, Integer> value : map.entrySet())
			{
				StringBuilder builder = new StringBuilder();
				builder.append(value.getKey());
				builder.append("#");
				builder.append(Integer.toString(value.getValue()));
				context.write(key, new Text(builder.toString()));
				System.out.println(builder.toString());
			}

		}
	}


	public static void main(
			String[] args)
			throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "file count");
		job.setJarByClass(FileCount.class);
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
