package mapreduce.donation.totalorderpartitioner;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import data.writable.DonationWritable;

public class TotalSortByState {
	
	public static class CompositeKeyCreationMapper extends Mapper<Text, DonationWritable, Text, DonationWritable> {

		Text outputKey = new Text();
		
		@Override
		public void map(Text key, DonationWritable donation, Context context) throws IOException, InterruptedException {
			
			// Ignore entries with empty values for better readability of results
			if (StringUtils.isEmpty(donation.donor_state) || StringUtils.isEmpty(donation.donor_city)) {
				return;
			}
			
			outputKey.set(donation.donor_state);
			context.write(outputKey, donation);
		}
		
	}

	public static class ValueOutputReducer extends Reducer<Text, DonationWritable, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<DonationWritable> donations, Context context) throws IOException, InterruptedException {
			
			for (DonationWritable donation : donations) {
				outputKey.set(donation.donation_id);
				outputValue.set(String.format("%s %s %.2f", donation.donor_state, donation.donor_city, donation.total));
				context.write(outputKey, outputValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		int nbReducers = 3;
				
		// Parse CLI parameters
		Path inputPath = new Path(args[0]);
		Path partitionInputPath = new Path(args[1]);
		Path partitionOutputPath = new Path(args[2]);
		Path outputPath = new Path(args[3]);
		
		/* 
		 * Sampling Job
		 */
		
		Job samplingJob = Job.getInstance(new Configuration(), "Sampling States");
		samplingJob.setJarByClass(TotalSortByState.class);

	    // The following instructions need to be executed before writing the partition file
		samplingJob.setNumReduceTasks(nbReducers);
	    FileInputFormat.setInputPaths(samplingJob, partitionInputPath);
	    FileOutputFormat.setOutputPath(samplingJob, partitionOutputPath);
		TotalOrderPartitioner.setPartitionFile(samplingJob.getConfiguration(), partitionOutputPath);
		samplingJob.setInputFormatClass(KeyValueTextInputFormat.class);
		samplingJob.setMapOutputKeyClass(Text.class);
		samplingJob.setMapOutputValueClass(Text.class);
		
		// Write partition file with random sampler
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
		InputSampler.writePartitionFile(samplingJob, sampler);
		
		
		/*
		 * MapReduce Job
		 */
		
		Job mrJob = Job.getInstance(new Configuration(), "Total Sorting on multiple reducers");
		mrJob.setJarByClass(TotalSortByState.class);
		
		// Partitioning configuration
		mrJob.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(mrJob.getConfiguration(), partitionOutputPath);
		mrJob.addCacheFile(partitionOutputPath.toUri());
		
		// Mapper configuration
		mrJob.setMapperClass(CompositeKeyCreationMapper.class);
	    mrJob.setInputFormatClass(SequenceFileInputFormat.class);
		mrJob.setMapOutputKeyClass(Text.class);
		mrJob.setMapOutputValueClass(DonationWritable.class);
		
		// Reducer configuration
		mrJob.setReducerClass(ValueOutputReducer.class);
		mrJob.setOutputKeyClass(Text.class);
		mrJob.setOutputValueClass(Text.class);
		mrJob.setNumReduceTasks(nbReducers);

	    FileInputFormat.setInputPaths(mrJob, inputPath);
	    FileOutputFormat.setOutputPath(mrJob, outputPath);
		
		System.exit(mrJob.waitForCompletion(true) ? 0 : 1);
		
	}

}
