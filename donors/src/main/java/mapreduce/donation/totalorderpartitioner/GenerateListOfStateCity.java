package mapreduce.donation.totalorderpartitioner;

import java.io.IOException;
import java.util.Random;

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

	private static final String RAND_SEPARATOR = ":";

	public static class RandomPrependingMapper extends Mapper<Object, DonationWritable, Text, Text> {

		private Text state = new Text();
		private Text city = new Text();
		private Random rand = new Random();

		@Override
		public void map(Object key, DonationWritable donation, Context context) throws IOException, InterruptedException {

			// Ignore rows where the donor state or city are not defined
			if (StringUtils.isEmpty((donation.donor_state)) || StringUtils.isEmpty(donation.donor_city)) {
				return;
			}

			state.set(rand.nextInt() + RAND_SEPARATOR + donation.donor_state);
			city.set(rand.nextInt() + RAND_SEPARATOR + donation.donor_city);
			context.write(state, city);
		}
	}

	public static class RandomTokenRemovalReducer extends Reducer<Text, Text, Text, Text> {

		private Text stateName = new Text();
		private Text cityName = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				stateName.set(StringUtils.substringAfter(key.toString(), RAND_SEPARATOR));
				cityName.set(StringUtils.substringAfter(value.toString(), RAND_SEPARATOR));
				context.write(stateName, cityName);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration(), "Generate unordered list of (State,City)");
		job.setJarByClass(GenerateListOfStateCity.class);

		// Mapper configuration
		job.setMapperClass(RandomPrependingMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reducer configuration
		job.setReducerClass(RandomTokenRemovalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(2);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
