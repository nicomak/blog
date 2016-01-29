package mapreduce.join.repartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.join.replicated.ReplicatedJoinBasic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import data.writable.DonationWritable;
import data.writable.ProjectWritable;

public class RepartitionJoinBasic {

	public static final Log LOG = LogFactory.getLog(ReplicatedJoinBasic.class);

	/**
	 * Donations Mapper.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class DonationsMapper extends Mapper<Object, DonationWritable, Text, ObjectWritable> {

		private Text outputKey = new Text();
		private ObjectWritable outputValue = new ObjectWritable();

		@Override
		public void map(Object key, DonationWritable donation, Context context) throws IOException, InterruptedException {
			
			outputKey.set(donation.project_id);
			outputValue.set(donation);
			context.write(outputKey, outputValue);			
		}
	}

	/**
	 * Projects Mapper.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class ProjectsMapper extends Mapper<Object, ProjectWritable, Text, ObjectWritable> {

		private Text outputKey = new Text();
		private ObjectWritable outputValue = new ObjectWritable();

		@Override
		public void map(Object offset, ProjectWritable project, Context context) throws IOException, InterruptedException {

			outputKey.set(project.project_id);
			outputValue.set(project);
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
	public static class JoinReducer extends Reducer<Text, ObjectWritable, Text, Text> {

		private static final String NULL_DONATION_OUTPUT = "null|null|null|null|null";
		private static final String NULL_PROJECT_OUTPUT = "null|null|null|null";

		private Text donationOutput = new Text();
		private Text projectOutput = new Text();

		private List<String> donationsList = new ArrayList<>();
		private List<String> projectsList = new ArrayList<>();

		@Override
		protected void reduce(Text projectId, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {

			// Clear data lists
			donationsList.clear();
			projectsList.clear();

			// Fill up data lists with selected fields
			for (ObjectWritable value : values) {
				Object object = value.get();

				if (object instanceof DonationWritable) {
					DonationWritable donation = (DonationWritable) object;
					String donationOutput = String.format("%s|%s|%s|%s|%.2f", donation.donation_id, donation.project_id, 
							donation.donor_city, donation.ddate, donation.total);
					donationsList.add(donationOutput);

				} else if (object instanceof ProjectWritable) {
					ProjectWritable project = (ProjectWritable) object;
					String projectOutput = String.format("%s|%s|%s|%s", project.project_id, project.school_city, 
							project.poverty_level, project.primary_focus_subject);
					projectsList.add(projectOutput);

				} else {
					String errorMsg = String.format("Object of class %s is neither a %s nor %s.", 
							object.getClass().getName(), ProjectWritable.class.getName(), DonationWritable.class.getName());
					throw new IOException(errorMsg);
				}
			}


			// Join data lists (example with FULL OUTER JOIN)
			if (!donationsList.isEmpty()) {

				for (String dontationStr : donationsList) {

					if (!projectsList.isEmpty()) {

						// Case 1 : Both LEFT and RIGHT sides of the join have values
						// Extra loop to write all combinations of (LEFT, RIGHT)
						// These are also the outputs of an INNER JOIN
						for (String projectStr : projectsList) {
							donationOutput.set(dontationStr);
							projectOutput.set(projectStr);
							context.write(donationOutput, projectOutput);
						}

					} else {

						// Case 2 : LEFT side has values but RIGHT side doesn't.
						// Simply write (LEFT, null) to output for each value of LEFT.
						// These are also the outputs of a LEFT OUTER JOIN
						donationOutput.set(dontationStr);
						projectOutput.set(NULL_PROJECT_OUTPUT);
						context.write(donationOutput, projectOutput);
					}
				}

			} else {

				// Case 3 : LEFT side doesn't have values, but RIGHT side has values
				// Simply write (null, RIGHT) to output for each value of LEFT.
				// These are also the outputs of a RIGHT OUTER JOIN
				for (String projectStr : projectsList) {
					donationOutput.set(NULL_DONATION_OUTPUT);
					projectOutput.set(projectStr);
					context.write(donationOutput, projectOutput);
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Repartition Join");
		job.setJarByClass(ReplicatedJoinBasic.class);

		// Input parameters
		Path donationsPath = new Path(args[0]);
		Path projectsPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		// Mappers configuration
		MultipleInputs.addInputPath(job, donationsPath, SequenceFileInputFormat.class, DonationsMapper.class);
		MultipleInputs.addInputPath(job, projectsPath, SequenceFileInputFormat.class, ProjectsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ObjectWritable.class);

		// Reducer configuration
		job.setNumReduceTasks(3);
		job.setReducerClass(JoinReducer.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
