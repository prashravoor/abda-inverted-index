import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
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

/*
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
 */

public class CosineQuery {

    public static class DocCounterMapper 
            extends Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            private final static Pattern PUNCT = Pattern.compile("\\p{Punct}");

            @Override
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    String[] keys = itr.nextToken().split("<>");
                    context.write(new Text(keys[1]), one);
                }
                    }
    }

    public static class DocCounterReducer 
            extends Reducer<Text, IntWritable, Text, Text> {

            public static enum Count {
                TOTAL_COUNT
            };

            @Override
            public void reduce(Text key, Iterable<IntWritable> values, 
                    Context context
                    ) throws IOException, InterruptedException {
                context.getCounter(Count.TOTAL_COUNT).increment(1);
                    }
    }

    public static class QueryIdfVectorMapper 
            extends Mapper<Object, Text, Text, Text>{

            private final static IntWritable one = new IntWritable(1);
            private final static Pattern PUNCT = Pattern.compile("\\p{Punct}");

            @Override
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                String query = context.getConfiguration().get("TERM");
                StringTokenizer qitr = new StringTokenizer(query.toString());

                StringTokenizer itr = new StringTokenizer(value.toString());
                String[] keys = itr.nextToken().split("<>");
                Double tfidf = Double.parseDouble(itr.nextToken().toString());

                while (qitr.hasMoreTokens()) {
                    String t = qitr.nextToken();
                    Matcher unwantedMatcher = PUNCT.matcher(t);
                    t = unwantedMatcher.replaceAll("");
                    if (t.equalsIgnoreCase(keys[0])) {
                        context.write(new Text(t), new Text(keys[1] + "<>" + tfidf.toString()));
                    }
                }
            }
    }

    public static class QueryIdfVectorReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

            @Override
            public void reduce(Text key, Iterable<Text> values, 
                    Context context
                    ) throws IOException, InterruptedException {
                String query = context.getConfiguration().get("TERM");
                StringTokenizer qitr = new StringTokenizer(query.toString());
                int numQueryTerms = 0;
                while ( qitr.hasMoreTokens() ) {
                    ++numQueryTerms;
                }

                int numOfDocuments = context.getConfiguration().getInt("TOTAL_DOCUMENTS", -1);

                int numDocsForWord = 0;
                for ( Text v : values ) {
                    ++numDocsForWord;
                }

                double tf = 1 / (double)numQueryTerms;
                double idf = Math.log10( numOfDocuments / (double)numDocsForWord);

                context.write(key, new DoubleWritable(tf * idf));
            }
    }

    public static class QueryCosineVectorMapper 
            extends Mapper<Object, Text, Text, Text>{

            @Override
            public void map(Object dummy, Text value, Context context
                    ) throws IOException, InterruptedException {
                String query = context.getConfiguration().get("TERM");

                StringTokenizer itr = new StringTokenizer(value.toString());
                String key = itr.nextToken();
                Double tfidf = Double.parseDouble(itr.nextToken().toString());

                context.write(new Text(query), new Text(key + "," + tfidf.toString()));
            }
    }

    public static class QueryCosineVectorReducer 
            extends Reducer<Text, Text, Text, Text>{

            @Override
            public void reduce(Text key, Iterable<Text> values, Context context
                    ) throws IOException, InterruptedException {

                String queryIdf = new String();
                for ( Text v : values) {
                    queryIdf += "<>" + v.toString();
                }

                queryIdf = queryIdf.substring(2);
                context.getConfiguration().set("QUERY_VECTOR", queryIdf);
            }
    }

    public static class QueryCosineMapper 
            extends Mapper<Object, Text, Text, Text>{

            @Override
            public void map(Object dummy, Text value, Context context
                    ) throws IOException, InterruptedException {
                String query = context.getConfiguration().get("QUERY_VECTOR");
                String[] queryVec = query.split("<>");

                StringTokenizer itr = new StringTokenizer(value.toString());
                String key = itr.nextToken();
                Double tfidf = Double.parseDouble(itr.nextToken().toString());

                String[] keys = key.split("<>");
                for( String k : queryVec ) {
                    String[] parts = k.split(",");
                    if ( parts[0].equalsIgnoreCase(keys[0]) ) {
                        context.write(new Text( query + "<>" + keys[1]), new Text("q," + parts[0] + "," + parts[1] + "<>" 
                                    + keys[1] + "," + parts[0] + "," + tfidf.toString()));
                    }
                }
            }
    }

    public static class QueryCosineReducer
            extends Reducer<Text, Text, DoubleWritable, Text> {

            @Override
            public void reduce(Text key, Iterable<Text> values, 
                    Context context
                    ) throws IOException, InterruptedException {

                double dotProduct = 0.0;
                double normQ = 0.0, normD = 0.0;
                for ( Text v : values ) {
                    String[] parts = v.toString().split("<>");

                    Double qidf = (Double.parseDouble(parts[0].split(",")[2]));
                    Double didf = (Double.parseDouble(parts[1].split(",")[2]));

                    dotProduct += qidf * didf;
                    normQ += (q*q);
                    normD += (d*d);
                }

                normQ = Math.sqrt(normQ);
                normD = Math.sqrt(normD);

                double cosine = dotProduct / (normQ * normD);

                context.write(new DoubleWritable(cosine), new Text(key.toString().split("<>")[1]));
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
        String outfile = otherArgs[otherArgs.length - 1];

        Job job = Job.getInstance(conf, "Count Documents");
        job.setJarByClass(CosineQuery.class);
        job.setMapperClass(DocCounterMapper.class);
        job.setReducerClass(DocCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 1; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(outfile + "_1"));

        if ( !job.waitForCompletion(true) ) {
            System.exit(1);
        }

        conf.setInt("TOTAL_DOCUMENTS", 
                (int)job.getCounters().findCounter(DocCounterReducer.Count.TOTAL_COUNT).getValue());

        Job job2 = Job.getInstance(conf, "Query IDF Vector Create");
        job2.setJarByClass(CosineQuery.class);
        job2.setMapperClass(QueryIdfVectorMapper.class);
        job2.setReducerClass(QueryIdfVectorReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        for (int i = 1; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job2,
                new Path(outfile + "_2"));

        if ( !job2.waitForCompletion(true) ) {
            System.exit(1);
        }

        Job job3 = Job.getInstance(conf, "Query Vector Generator");
        job3.setJarByClass(CosineQuery.class);
        job3.setMapperClass(QueryCosineVectorMapper.class);
        job3.setReducerClass(QueryCosineVectorReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(outfile + "_2"));
        FileOutputFormat.setOutputPath(job,
                new Path(outfile + "_3"));

        if ( !job3.waitForCompletion(true) ) {
            System.exit(1);
        }

        Job job4 = Job.getInstance(conf, "Cosine Similarity Generator");
        job4.setJarByClass(CosineQuery.class);
        job4.setMapperClass(QueryCosineMapper.class);
        job4.setReducerClass(QueryCosineReducer.class);
        job4.setOutputKeyClass(DoubleWritable.class);
        job4.setOutputValueClass(Text.class);
        for (int i = 1; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(outfile));

        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
