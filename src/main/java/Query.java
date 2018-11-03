import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import querying.QueryVectorizer;
import querying.Relevance;

public class Query {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    final Configuration conf = new Configuration();
    final Path relevantPath = new Path(String.format("%s%s", "relevanted", args[0]));
    final Path vectorizedPath = new Path(String.format("%s%s", "vectorized", args[0]));
    final Path queryFile = new Path(String.format("%s", args[0]));  // Query folder
    final Path vocabularyPath = new Path(String.format("%s", args[1]));  // Merge output
    final Path indexerPath = new Path(String.format("%s", args[2]));  // Indexer output

    Job jobVectorize = Job.getInstance(conf, "Query vectorizer task");
    jobVectorize.setJarByClass(QueryVectorizer.class);
    jobVectorize.setMapperClass(QueryVectorizer.TokenizerMapper.class);
    jobVectorize.setReducerClass(QueryVectorizer.IntSumReducer.class);
    jobVectorize.setOutputKeyClass(Text.class);
    jobVectorize.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(jobVectorize, vocabularyPath, queryFile);
    FileOutputFormat.setOutputPath(jobVectorize, vectorizedPath);
    if (jobVectorize.waitForCompletion(true)) {
      System.exit(1);
    }

    Job jobRelevance = Job.getInstance(conf, "Relevance task");
    jobRelevance.setJarByClass(Relevance.class);
    jobRelevance.setMapperClass(Relevance.TokenizerMapper.class);
    jobRelevance.setReducerClass(Relevance.IntSumReducer.class);
    jobRelevance.setOutputKeyClass(Text.class);
    jobRelevance.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(jobRelevance, vectorizedPath, indexerPath);
    FileOutputFormat.setOutputPath(jobRelevance, relevantPath);
    if (!jobRelevance.waitForCompletion(true)) {
      System.exit(1);
    }

    Job extractorJob = Job.getInstance(conf, "Extractor task");
    extractorJob.setJarByClass(Relevance.class);
    extractorJob.setMapperClass(Relevance.TokenizerMapper.class);
    extractorJob.setReducerClass(Relevance.IntSumReducer.class);
    extractorJob.setOutputKeyClass(Text.class);
    extractorJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(extractorJob, relevantPath);
    FileOutputFormat.setOutputPath(extractorJob, new Path(args[3]));
    if (!extractorJob.waitForCompletion(true)) {
      System.exit(1);
    }
  }
}
