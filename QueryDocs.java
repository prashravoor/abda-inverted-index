import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class SortFloatComparator extends WritableComparator {

	protected SortFloatComparator() {
		super(DoubleWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DoubleWritable k1 = (DoubleWritable)w1;
		DoubleWritable k2 = (DoubleWritable)w2;
		
		return -1 * k1.compareTo(k2);
	}
}

public class QueryDocs {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private final static Pattern PUNCT = Pattern.compile("\\p{Punct}");
      
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String query = context.getConfiguration().get("TERM");
      StringTokenizer qitr = new StringTokenizer(query.toString());

      StringTokenizer itr = new StringTokenizer(value.toString());
      String[] keys = itr.nextToken().split("<>");
      double tfidf = Double.parseDouble(itr.nextToken().toString());

      while (qitr.hasMoreTokens()) {
        String t = qitr.nextToken();
        Matcher unwantedMatcher = PUNCT.matcher(t);
        t = unwantedMatcher.replaceAll("");
        if (t.equalsIgnoreCase(keys[0])) {
            context.write(new Text(keys[1]), new DoubleWritable(tfidf));
        }
      }
    }
  }
  
  public static class QueryReducer 
       extends Reducer<Text, DoubleWritable, DoubleWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        double sum = 0;
        for ( DoubleWritable v : values ) {
            sum += v.get();
        }

        context.write(new DoubleWritable(sum), new Text(key.toString()));
     }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: query <term> <in> [<in>...] <out>");
      System.exit(2);
    }

    conf.set("TERM", otherArgs[0]);
    
    Job job = Job.getInstance(conf, "Query");
    job.setJarByClass(QueryDocs.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(QueryReducer.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setSortComparatorClass(SortFloatComparator.class);
    for (int i = 1; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
