package mapreduce.join.bloomfilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.join.replicated.ReplicatedJoinBasic;

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

import data.writable.DonationWritable;
import data.writable.ProjectWritable;

public class RepartitionJoinScienceProjects {

	public static final Log LOG = LogFactory.getLog(ReplicatedJoinBasic.class);

	/**
	 * Donations Mapper.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class DonationsMapper extends Mapper<Object, DonationWritable, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(Object key, DonationWritable donation, Context context) throws IOException, InterruptedException {

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

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(Object offset, ProjectWritable project, Context context) throws IOException, InterruptedException {

			// Ignore projects where the primary subject is not about science
			String subject = (project.primary_focus_subject != null) ? project.primary_focus_subject.toLowerCase() : "";
			if (!subject.contains("science")) {
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
		Job job = Job.getInstance(conf, "Repartition Join (science projects only)");
		job.setJarByClass(ReplicatedJoinBasic.class);

		// Input parameters
		Path donationsPath = new Path(args[0]);
		Path projectsPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		// Mappers configuration
		MultipleInputs.addInputPath(job, donationsPath, SequenceFileInputFormat.class, DonationsMapper.class);
		MultipleInputs.addInputPath(job, projectsPath, SequenceFileInputFormat.class, ProjectsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reducer configuration
		job.setNumReduceTasks(3);
		job.setReducerClass(JoinReducer.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
