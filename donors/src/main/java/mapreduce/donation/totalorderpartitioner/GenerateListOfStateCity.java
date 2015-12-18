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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import data.writable.DonationWritable;

public class GenerateListOfStateCity {
	
	public static class StateCityMapper extends Mapper<Object, DonationWritable, Text, Text> {

	    private Text state = new Text();
	    private Text city = new Text();
		
		@Override
		public void map(Object key, DonationWritable donation, Context context) throws IOException, InterruptedException {
			
			// Ignore rows where the donor state or city are not defined
			if (StringUtils.isEmpty((donation.donor_state)) || StringUtils.isEmpty(donation.donor_city)) {
				return;
			}
			
			state.set(donation.donor_state);
			city.set(donation.donor_city);
			context.write(state, city);
		}
	}

	public static void main(String[] args) throws Exception {
		
		Job job = Job.getInstance(new Configuration(), "Generate list of State-City");
		job.setJarByClass(GenerateListOfStateCity.class);
		
		// Mapper configuration
		job.setMapperClass(StateCityMapper.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reducer configuration (using default identity reducer)
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
