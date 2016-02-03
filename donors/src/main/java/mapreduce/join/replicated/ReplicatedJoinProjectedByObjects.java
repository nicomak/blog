package mapreduce.join.replicated;

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

import data.writable.DonationWritable;
import data.writable.ProjectWritable;

public class ReplicatedJoinProjectedByObjects {

	public static final String PROJECTS_FILENAME_CONF_KEY = "projects.filename";

	public static final Log LOG = LogFactory.getLog(ReplicatedJoinProjectedByObjects.class);

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

		private Map<String, ProjectProjection> projectsCache = new HashMap<>();

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

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

							// We are creating a smaller projection object here to save memory space.
							ProjectProjection projection = new ProjectProjection(tempValue);
							projectsCache.put(tempKey.toString(), projection);
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

			ProjectProjection project = projectsCache.get(donation.project_id);

			// Ignore if the corresponding entry doesn't exist in the projects data (INNER JOIN)
			if (project == null) {
				return;
			}

			if (project != null) {
				String donationOutput = String.format("%s|%s|%s|%s|%.2f", donation.donation_id, donation.project_id, 
						donation.donor_city, donation.ddate, donation.total);

				String projectOutput = String.format("%s|%s|%s|%s", 
						project.project_id, project.school_city, project.poverty_level, project.primary_focus_subject);

				outputKey.set(donationOutput);
				outputValue.set(projectOutput);
				context.write(outputKey, outputValue);
			}
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
		Job job = Job.getInstance(conf, "Replicated Join (with projection)");
		job.setJarByClass(ReplicatedJoinProjectedByObjects.class);

		// Input parameters
		Path donationsPath = new Path(args[0]);
		Path projectsPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		// Add the projects sequence file to cached files
		job.addCacheFile(projectsPath.toUri());
		job.getConfiguration().set(PROJECTS_FILENAME_CONF_KEY, projectsPath.getName());

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
