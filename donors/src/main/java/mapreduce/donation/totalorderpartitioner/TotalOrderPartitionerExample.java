package mapreduce.donation.totalorderpartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class TotalOrderPartitionerExample {

	public static void main(String[] args) throws Exception {

		// Parse CLI parameters
		Path inputPath = new Path(args[0]);
		Path partitionOutputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		Job job = Job.getInstance(new Configuration(), "Sampling States");
		job.setJarByClass(TotalOrderPartitionerExample.class);

	    // The following instructions need to be executed before writing the partition file
		job.setNumReduceTasks(3);
	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Write partition file with random sampler
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
		InputSampler.writePartitionFile(job, sampler);

		// Use TotalOrderPartitioner and default identity mapper and reducer 
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
