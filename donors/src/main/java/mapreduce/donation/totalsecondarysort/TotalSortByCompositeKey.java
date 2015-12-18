package mapreduce.donation.totalsecondarysort;

import java.io.IOException;

import mapreduce.donation.secondarysort.CompositeKey;
import mapreduce.donation.secondarysort.FullKeyComparator;
import mapreduce.donation.secondarysort.NaturalKeyComparator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import data.writable.DonationWritable;

public class TotalSortByCompositeKey {

	public static final Log LOG = LogFactory.getLog(TotalSortByCompositeKey.class);
	
	public static class CompositeKeyCreationMapper extends Mapper<Object, DonationWritable, CompositeKey, DonationWritable> {

		CompositeKey outputKey = new CompositeKey();
		
		@Override
		public void map(Object key, DonationWritable donation, Context context) throws IOException, InterruptedException {
			
			// Ignore entries with empty values for better readability of results
			if (StringUtils.isEmpty(donation.donor_state) || StringUtils.isEmpty(donation.donor_city)) {
				return;
			}
			
			outputKey.set(donation.donor_state, donation.donor_city, donation.total);
			context.write(outputKey, donation);
		}
		
	}

	public static class ValueOutputReducer extends Reducer<CompositeKey, DonationWritable, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		public void reduce(CompositeKey key, Iterable<DonationWritable> donations, Context context) throws IOException, InterruptedException {
			
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
		Path partitionOutputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		/* 
		 * Sampling Job
		 */
		
		Job samplingJob = Job.getInstance(new Configuration(), "Sampling Composite Keys");
		samplingJob.setJarByClass(TotalSortByCompositeKey.class);

	    // The following instructions need to be executed before writing the partition file
 		samplingJob.setNumReduceTasks(nbReducers);
 	    FileInputFormat.setInputPaths(samplingJob, inputPath);
 	    FileOutputFormat.setOutputPath(samplingJob, partitionOutputPath);
 		TotalOrderPartitioner.setPartitionFile(samplingJob.getConfiguration(), partitionOutputPath);
 		samplingJob.setMapOutputKeyClass(CompositeKey.class);
 		samplingJob.setMapOutputValueClass(NullWritable.class);
 		
 		// Custom input format
 		samplingJob.setInputFormatClass(CompositeKeySamplingInputFormat.class);
 		CompositeKeySamplingInputFormat.setNbSamples(samplingJob, 250);
 		CompositeKeySamplingInputFormat.setReadLimit(samplingJob, 500000);
 		
 		// Write partition file with random sampler
 		InputSampler.Sampler<CompositeKey, NullWritable> sampler = new InputSampler.SplitSampler<>(1000);
 		InputSampler.writePartitionFile(samplingJob, sampler);
	 	
 		
		/*
		 * MapReduce Job
		 */
		
		Job mrJob = Job.getInstance(new Configuration(), "Total Sorting on CompositeKey");
		mrJob.setJarByClass(TotalSortByCompositeKey.class);
				
		// Partitioning configuration
		mrJob.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(mrJob.getConfiguration(), partitionOutputPath);
		mrJob.addCacheFile(partitionOutputPath.toUri());
		
		// Sorting and Grouping comparators for composite key
		mrJob.setSortComparatorClass(FullKeyComparator.class);
		mrJob.setGroupingComparatorClass(NaturalKeyComparator.class);
		
		// Mapper configuration
		mrJob.setMapperClass(CompositeKeyCreationMapper.class);
	    mrJob.setInputFormatClass(SequenceFileInputFormat.class);
		mrJob.setMapOutputKeyClass(CompositeKey.class);
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
