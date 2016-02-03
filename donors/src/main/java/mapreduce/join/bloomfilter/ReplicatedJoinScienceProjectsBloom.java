package mapreduce.join.bloomfilter;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import data.writable.DonationWritable;
import data.writable.ProjectWritable;

public class ReplicatedJoinScienceProjectsBloom {

	public static final String PROJECTS_FILENAME_CONF_KEY = "projects.filename";
	public static final String PROJECTS_FILTER_FILE = "projects.bloomfilter.filename";
	public static final Log LOG = LogFactory.getLog(ReplicatedJoinScienceProjectsBloom.class);

	/**
	 * Mapper which does the joining.
	 * This mapper reads an input split of the "donations" dataset, 
	 * and joins its records with matching records from the "projects" dataset,
	 * which is pre-loaded from the distributed cache.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class ReplicatedJoinMapper extends Mapper<Object, DonationWritable, Text, Text> {

		private Map<String, String> projectsCache = new HashMap<>();
		private BloomFilter filter = new BloomFilter();
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			/************************
		        Load bloom filter
			 *************************/

			String bloomFilterCacheFile = context.getConfiguration().get(PROJECTS_FILTER_FILE);
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


			/***********************
			   Load projects cache
			 ************************/

			boolean cacheOK = false;

			URI[] cacheFiles = context.getCacheFiles();
			final String distributedCacheFilename = context.getConfiguration().get(PROJECTS_FILENAME_CONF_KEY);

			Configuration conf = new Configuration();
			for (URI cacheFile : cacheFiles) {
				Path path = new Path(cacheFile);
				if (path.getName().equals(distributedCacheFilename)) {
					LOG.info("Starting to build cache from : " + cacheFile);
					try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path))) 
					{
						LOG.info("Compressed ? " + reader.isBlockCompressed());
						Text tempKey = new Text();
						ProjectWritable tempValue = new ProjectWritable();

						while (reader.next(tempKey, tempValue)) {

							// The bloom filter returns true if the given project's subject is about Science, possibly with false positives
							Key projectIdKey = new Key(tempValue.project_id.getBytes());
							boolean possibilityOfScience = filter.membershipTest(projectIdKey);
							if (!possibilityOfScience) {
								return;
							}

							// Serialize important values to a string containing pipe-separated values
							String projectString = String.format("%s|%s|%s|%s", tempValue.project_id, 
									tempValue.school_city, tempValue.poverty_level, tempValue.primary_focus_subject);

							// Add to cache
							projectsCache.put(tempKey.toString(), projectString);
						}
					}
					LOG.info("Finished to build cache. Number of entries : " + projectsCache.size());

					cacheOK = true;
					break;
				}
			}

			if (!cacheOK) {
				LOG.error("Distributed cache file not found : " + distributedCacheFilename);
				throw new IOException("Distributed cache file not found : " + distributedCacheFilename);
			}
		}

		@Override
		public void map(Object key, DonationWritable donation, Context context)
				throws IOException, InterruptedException {

			String projectOutput = projectsCache.get(donation.project_id);

			// Ignore if the corresponding entry doesn't exist in the projects data (INNER JOIN)
			if (projectOutput == null) {
				return;
			}

			String donationOutput = String.format("%s|%s|%s|%s|%.2f", donation.donation_id, donation.project_id, 
					donation.donor_city, donation.ddate, donation.total);

			outputKey.set(donationOutput);
			outputValue.set(projectOutput);
			context.write(outputKey, outputValue);
		}
	}

	/**
	 * Smaller "Project" class used for projection.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class ProjectProjection {

		public String project_id;
		public String school_city;
		public String poverty_level;
		public String primary_focus_subject;

		public ProjectProjection(ProjectWritable project) {
			this.project_id = project.project_id;
			this.school_city = project.school_city;
			this.poverty_level = project.poverty_level;
			this.primary_focus_subject = project.primary_focus_subject;
		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Replicated Join (science projects with bloom filter)");
		job.setJarByClass(ReplicatedJoinScienceProjectsBloom.class);

		// Input parameters
		Path donationsPath = new Path(args[0]);
		Path projectsPath = new Path(args[1]);
		Path projectsBloomFilter = new Path(args[2]);
		Path outputPath = new Path(args[3]);

		// Add the projects sequence file to cached files
		job.addCacheFile(projectsPath.toUri());
		job.getConfiguration().set(PROJECTS_FILENAME_CONF_KEY, projectsPath.getName());

		// Add the bloom filter to cached files
		job.addCacheFile(projectsBloomFilter.toUri());
		job.getConfiguration().set(PROJECTS_FILTER_FILE, projectsBloomFilter.getName());

		// Mapper configuration
		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// No need to reduce
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, donationsPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
