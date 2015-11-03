package spark.wordcount;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.ArrayList;

/**
* Spark Word Count class, taken from the Apache Spark official examples.
* The FlatMap function has been modified for my benchmark.
*/
public final class SparkWordCount {

    private static Pattern specialCharsRemovePattern = Pattern.compile("[^a-zA-Z]");

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: SparkWordCount <hdfs_hostname:port> <input_dir> <output_dir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile("hdfs://" + args[0] + "/" + args[1]);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                String[] wordsArray = specialCharsRemovePattern.matcher(s).replaceAll(" ").toLowerCase().split("\\s+");
                List<String> filteredWords = new ArrayList<String>();
                for (String word : wordsArray) {
                    if (word.length() >= 50) continue;
                    filteredWords.add(word);
                }
                return filteredWords;
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }

            JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer i1, Integer i2) {
                    return i1 + i2;
                }
            });

            counts.saveAsTextFile("hdfs://" + args[0] + "/" + args[2]);
            
            ctx.stop();
        }
    }
}
