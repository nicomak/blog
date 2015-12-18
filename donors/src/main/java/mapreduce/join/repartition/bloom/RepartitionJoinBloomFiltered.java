package mapreduce.join.repartition.bloom;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import data.writable.DonationWritable;
import data.writable.ProjectWritable;

public class RepartitionJoinBloomFiltered {
		
	public static final Log LOG = LogFactory.getLog(RepartitionJoinBloomFiltered.class);
	
	/**
	 * Donations Mapper.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class DonationsMapper extends Mapper<Object, DonationWritable, Text, Text> {

		public static final String DONATION_FILTER_FILE = "bloomfilter.donationamount.filename";
		
		private BloomFilter filter = new BloomFilter();
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			String bloomFilterCacheFile = context.getConfiguration().get(DONATION_FILTER_FILE);
			
			try (
				FileInputStream fis = new FileInputStream(bloomFilterCacheFile);
				DataInputStream dis = new DataInputStream(fis);
			) 
			{
				filter.readFields(dis);
			
			} catch (Exception e) {
				throw new IOException("Error while reading bloom filter from file system.", e);
			}
			
			LOG.info("Finished to read filter with vector size : " + filter.getVectorSize());
		}
		
		@Override
		public void map(Object key, DonationWritable donation, Context context) throws IOException, InterruptedException {
			
			// The bloom filter returns if the given project's subject is about Science, with possibly false positives 
			boolean possibilityOfScience = filter.membershipTest(new Key(donation.project_id.getBytes()));
			
			// Ignore results where the total donation is less than 100$,
			// or when it is absolutely sure that the subject is not about Science
			if (donation.total < 100 || !possibilityOfScience) {
				return;
			}
			
			outputKey.set(donation.project_id);
			String donationOutput = String.format("D|%s|%s|%s|%s|%.2f", donation.donation_id, 
					donation.project_id, donation.ddate, donation.donor_city, donation.total);
			outputValue.set(donationOutput);
			context.write(outputKey, outputValue);
		}
	}
	
	/**
	 * Projects Mapper.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class ProjectsMapper extends Mapper<Object, ProjectWritable, Text, Text> {

		public static final String DONATION_FILTER_FILE = "bloomfilter.donationamount.filename";
		
		private BloomFilter filter = new BloomFilter();
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			String bloomFilterCacheFile = context.getConfiguration().get(DONATION_FILTER_FILE);
			
			try (
				FileInputStream fis = new FileInputStream(bloomFilterCacheFile);
				DataInputStream dis = new DataInputStream(fis);
			) 
			{
				filter.readFields(dis);
			
			} catch (Exception e) {
				throw new IOException("Error while reading bloom filter from file system.", e);
			}
			
			LOG.info("Finished to read filter with vector size : " + filter.getVectorSize());
		}
		
		@Override
		public void map(Object offset, ProjectWritable project, Context context) throws IOException, InterruptedException {
			
			// We have all the information on the project entries to filter out data correctly (without False Positives), 
			// but let's still use the BloomFilter, because it will save time on string transformation and searching.
			
			// The bloom filter returns if the given project's subject is about Science, with possibly false positives 
			boolean possibilityOfScience = filter.membershipTest(new Key(project.project_id.getBytes()));
			if (!possibilityOfScience) {
				return;
			}
			
			outputKey.set(project.project_id);
			// Create new object with projected values
			String projectOutput = String.format("P|%s|%s|%s|%s", 
					project.project_id, project.school_city, project.poverty_level, project.primary_focus_subject);
			outputValue.set(projectOutput);
			context.write(outputKey, outputValue);
			
		}
	}
	
	/**
	 * Join Reducer.
	 * Each invocation of the reduce() method will receive a list of ObjectWritable.
	 * These ObjectWritable objects are either Donation or Project objects,
	 * and are all linked by the same "project_id".
	 * 
	 * @author Nicomak
	 *
	 */
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text donationOutput = new Text();
		private Text projectOutput = new Text();
		
		private List<String> donationsList;
		private List<String> projectsList;
		
		@Override
		protected void reduce(Text projectId, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Clear data lists
			donationsList = new ArrayList<>();
			projectsList = new ArrayList<>();
			
			// Fill up data lists with selected fields
			for (Text value : values) {
				String textVal = value.toString();
				
				// Get first char which determines the type of data
				char type = textVal.charAt(0);
				
				// Remove the type flag "P|" or "D|" from the beginning to get original data content
				textVal = textVal.substring(2);
				
				if (type == 'D') {
					donationsList.add(textVal);
				
				} else if (type == 'P') {
					projectsList.add(textVal);
					
				} else {
					String errorMsg = String.format("Type is neither a D nor P.");
					throw new IOException(errorMsg);
				}
			}
			
			// Join data lists only if both sides exist (INNER JOIN)
			if (!donationsList.isEmpty() && !projectsList.isEmpty()) {

				// Write all combinations of (LEFT, RIGHT) values
				for (String dontationStr : donationsList) {
					for (String projectStr : projectsList) {
						donationOutput.set(dontationStr);
						projectOutput.set(projectStr);
						context.write(donationOutput, projectOutput);
					}
				}
			}
			
		}		
	}	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Repartition Join Bloom Filtered");
		job.setJarByClass(RepartitionJoinBloomFiltered.class);
		
		// Input parameters
		Path donationsPath = new Path(args[0]);
		Path projectsPath = new Path(args[1]);
		Path donationBloomFilter = new Path(args[2]);
		Path outputPath = new Path(args[3]);
		
		// Create cache file and set path in configuration to be retrieved later by the mapper
	    job.addCacheFile(donationBloomFilter.toUri());
	    job.getConfiguration().set(DonationsMapper.DONATION_FILTER_FILE, donationBloomFilter.getName());

		// Mappers configuration
		MultipleInputs.addInputPath(job, donationsPath, SequenceFileInputFormat.class, DonationsMapper.class);
		MultipleInputs.addInputPath(job, projectsPath, SequenceFileInputFormat.class, ProjectsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reducer configuration
		job.setNumReduceTasks(1);
		job.setReducerClass(JoinReducer.class);
		
	    FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
