package mapreduce.join.bloomfilter;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import data.writable.ProjectWritable;

public class ProjectFilterTest {

	public static final String PROJECTS_FILTER_FILE = "projects.bloomfilter.filename";
	public static final Log LOG = LogFactory.getLog(ProjectFilterTest.class);
	
	/**
	 * @author Nicomak
	 *
	 */
	public static class TestProjectBloomFilterMapper extends Mapper<Object, ProjectWritable, Text, Text> {
		
		public static final String DONATION_FILTER_FILE = "bloomfilter.donationamount.filename";
		
		private BloomFilter filter = new BloomFilter();
		
		private static final Text FP_KEY = new Text("FALSE_POSITIVE");
		private static final Text FN_KEY = new Text("FALSE_NEGATIVE");
		
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
		public void map(Object key, ProjectWritable project, Context context) throws IOException, InterruptedException {

			boolean membership = filter.membershipTest(new Key(project.project_id.getBytes()));
			String subject = (project.primary_focus_subject != null) ? project.primary_focus_subject.toLowerCase() : "";
			
			if (subject.contains("science") && !membership) {
				
				// The subject contains "science", but the BloomFilter says it does not => False Negative
				outputValue.set(project.project_id);
				context.write(FN_KEY, outputValue);
			
			} else if (!subject.contains("science") && membership) {
				
				// The subject does not contain "science", but the BloomFilter says it does => False Positive
				outputValue.set(project.project_id);
				context.write(FP_KEY, outputValue);
				
			}
			
		}		
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Donation BloomFilter Test");
		job.setJarByClass(ProjectFilterTest.class);
		
		// Input parameters
		Path donationsPath = new Path(args[0]);
		Path donationsBloomFilter = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		// Create cache file and set path in configuration to be retrieved later by the mapper
	    job.addCacheFile(donationsBloomFilter.toUri());
	    job.getConfiguration().set(PROJECTS_FILTER_FILE, donationsBloomFilter.getName());

		// Mapper configuration
		job.setMapperClass(TestProjectBloomFilterMapper.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reducer configuration
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, donationsPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
