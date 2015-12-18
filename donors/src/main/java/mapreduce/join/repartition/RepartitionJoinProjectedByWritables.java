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

public class RepartitionJoinProjectedByWritables {
		
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
			
			// Ignore results where the total donation is less than 100$ 
			if (donation.total < 100) {
				return;
			}
			
			outputKey.set(donation.project_id);
			// Create new object with projected values
			outputValue.set(DonationProjection.makeProjection(donation));
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
			
			// Ignore projects where the primary subject is not about science
			String subject = (project.primary_focus_subject != null) ? project.primary_focus_subject.toLowerCase() : "";
			if (!subject.contains("science")) {
				return;
			}
			
			outputKey.set(project.project_id);
			// Create new object with projected values
			outputValue.set(ProjectProjection.makeProjection(project));
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
		
		private Text donationOutput = new Text();
		private Text projectOutput = new Text();
		
		private List<String> donationsList;
		private List<String> projectsList;
		
		@Override
		protected void reduce(Text projectId, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {

			// Clear data lists
			donationsList = new ArrayList<>();
			projectsList = new ArrayList<>();
			
			// Fill up data lists with selected fields
			for (ObjectWritable value : values) {
				Object object = value.get();
				
				if (object instanceof DonationProjection) {
					DonationProjection donation = (DonationProjection) object;
					String donationOutput = String.format("%s|%s|%s|%s|%.2f", donation.donation_id, 
							donation.project_id, donation.ddate, donation.donor_city, donation.total);
					donationsList.add(donationOutput);
				
				} else if (object instanceof ProjectProjection) {
					ProjectProjection project = (ProjectProjection) object;
					String projectOutput = String.format("%s|%s|%s|%s", 
							project.project_id, project.school_city, project.poverty_level, project.primary_focus_subject);
					projectsList.add(projectOutput);
					
				} else {
					String errorMsg = String.format("Object of class %s is neither a %s nor %s.", 
							object.getClass().getName(), ProjectWritable.class.getName(), DonationWritable.class.getName());
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
		job.setNumReduceTasks(1);
		job.setReducerClass(JoinReducer.class);
		
	    FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
