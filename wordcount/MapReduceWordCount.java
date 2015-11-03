import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
* MapReduce Word Count class, taken from the Apache Hadoop MapReduce tutorial.
* The map function was slightly modified for my benchmark.
*/
public class MapReduceWordCount {

    /**
    * Mapper Class.
    */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static Pattern specialCharsRemovePattern = Pattern.compile("[^a-zA-Z]");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = specialCharsRemovePattern.matcher(value.toString()).replaceAll(" ").toLowerCase().split("\\s+");
            for (String token : tokens) {
                if (token.length() >= 50) continue;
                word.set(token);
                context.write(word, one);
            }
        }
    }

    /**
    * Reducer Class.
    */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
    * Main method.
    */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
