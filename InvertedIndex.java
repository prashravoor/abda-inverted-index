import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, LongWritable>{
    
    private final static LongWritable one = new LongWritable(1);
    private final static Pattern PUNCT = Pattern.compile("\\p{Punct}");
      
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String word = itr.nextToken();
        // word = word.replaceAll("(?:--|'#%$&,;.!\"[\\[\\]{}()+/\\\\])", "");
        Matcher unwantedMatcher = PUNCT.matcher(word);
        word = unwantedMatcher.replaceAll("");
        if (!word.isEmpty())
            context.write(new Text(word.toString() + "<>" + filename), one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,LongWritable,Text,LongWritable> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        int sum = 0;
        for ( LongWritable v : values ) {
          sum += v.get();
        }
        
        context.write(key, new LongWritable(sum));
     }
  }

  public static class MapWordCountToDoc
       extends Mapper<Object, Text, Text, Text>{
    
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String[] keys = itr.nextToken().split("<>");
      String t = keys[0] + "<>" + itr.nextToken();
      // System.out.println("Key: " + keys[1] + ": Value:" + t);
      context.write(new Text(keys[1]), new Text(t));
    }
  }
  
  public static class CountWordsInDocReducer
       extends Reducer<Text,Text,Text,Text> {

    public static enum Count {
        TOTAL_COUNT
    };

    @Override
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        long sum = 0;
        String docname = key.toString();
        // System.out.println("Docname: " + docname);

        context.getCounter(Count.TOTAL_COUNT).increment(1);

        Map<String, Long> words = new HashMap<>();
        for ( Text val : values ) {
          // System.out.println("Value: " + val);
          String[] parts = val.toString().split("<>");
          long n = Long.parseLong(parts[1]);
          sum += n;
          // System.out.println("In Words: " + parts[0]);
          words.put(docname + "<>" + parts[0], n);
        }
        // System.out.println("Sum: " + sum);
    
        for ( String val : words.keySet() ) {
          long n = words.get(val);
          // System.out.println("Value: " + val + "," + n);
          context.write(new Text(val), 
              new Text(Long.toString(n) + "<>" + Long.toString(sum)));
        }
    }
  }

  public static class CalcTermFrequencyMapper extends Mapper<Object, Text, Text, Text>
  {
      @Override
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          StringTokenizer itr = new StringTokenizer(value.toString());
          String[] keys = itr.nextToken().split("<>");
          String[] vals = itr.nextToken().split("<>");

          context.write(new Text(keys[1]), new Text(keys[0] + "<>" + vals[0] + "<>" + vals[1] + "<>" + 1));
      }
  }

  public static class CalcTermFrequencyReducer
       extends Reducer<Text,Text,Text,DoubleWritable> {

    private int numOfDocuments = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.numOfDocuments = conf.getInt("TOTAL_DOCUMENTS", -1);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        int sum = 0;
        Map<String, Vector<Long>> docCount = new HashMap<>();
        for ( Text val : values ) {
          String[] parts = val.toString().split("<>");
          if ( 4 != parts.length )
              continue;
          long n = Long.parseLong(parts[1]);
          long N = Long.parseLong(parts[2]);
          long m = Long.parseLong(parts[3]);
          sum += m;
          Vector<Long> l = new Vector<>();
          l.add(n);
          l.add(N);
          docCount.put(key.toString() + "<>" + parts[0], l);
        }

        Map<String, Double> tfidf = new HashMap<>();
        for (String val : docCount.keySet()) {
            double tf, idf, result;
            if (docCount.get(val).get(1) == 0) {
                continue;
            }

            tf = ((double)docCount.get(val).get(0) / (double)docCount.get(val).get(1));
            if ( sum == 0 ) {
                idf = 0.0;
            } else {
                idf = java.lang.Math.log10( (double)( numOfDocuments/((double)(sum)) ) );
            }
            result = tf * idf;
            // System.out.println("Word, Doc: " + val + ", tf = " + Double.toString(tf) + ", idf = " + idf);
            tfidf.put(val, result);
        }
    
        for ( String val : tfidf.keySet() ) {
          context.write(new Text(val), new DoubleWritable(tfidf.get(val)));
        }
    }
  }

   public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word Frequency Count");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(16);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1] + "_1"));
    job.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "Count Words in Document");
    job2.setJarByClass(InvertedIndex.class);
    job2.setMapperClass(MapWordCountToDoc.class);
    // job2.setCombinerClass(CountWordsInDocReducer.class);
    job2.setReducerClass(CountWordsInDocReducer.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 1] + "_1/part-r-00000"));
    FileOutputFormat.setOutputPath(job2,
      new Path(otherArgs[otherArgs.length - 1] + "_2"));
    job2.waitForCompletion(true);
    conf.setInt("TOTAL_DOCUMENTS", 
        (int)job2.getCounters().findCounter(CountWordsInDocReducer.Count.TOTAL_COUNT).getValue());

    Job job3 = Job.getInstance(conf, "Calculate Inverted Index");
    job3.setJarByClass(InvertedIndex.class);
    job3.setMapperClass(CalcTermFrequencyMapper.class);
    // job3.setCombinerClass(CalcTermFrequencyReducer.class);
    job3.setReducerClass(CalcTermFrequencyReducer.class);
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job3, new Path(otherArgs[otherArgs.length - 1] + "_2/part-r-00000"));
    FileOutputFormat.setOutputPath(job3,
      new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
