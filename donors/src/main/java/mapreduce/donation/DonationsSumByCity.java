package mapreduce.donation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import data.writable.DonationWritable;

public class DonationsSumByCity {

	public static class CityDonationMapper extends Mapper<Text, DonationWritable, Text, FloatWritable> {

		private Text city = new Text();
		private FloatWritable total = new FloatWritable();

		@Override
		public void map(Text key, DonationWritable donation, Context context) throws IOException, InterruptedException {

			// Ignore rows where the donor is a teacher
			if ("t".equals(donation.donor_is_teacher)) {
				return;
			}

			// Transform city name to uppercase and write the (string, float) pair.
			city.set(donation.donor_city.toUpperCase());
			total.set(donation.total);
			context.write(city, total);
		}
	}

	public static class FloatSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		private FloatWritable result = new FloatWritable();

		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float sum = 0;
			for (FloatWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration(), "Sum donations by city");
		job.setJarByClass(DonationsSumByCity.class);

		// Mapper configuration
		job.setMapperClass(CityDonationMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		// Reducer configuration (use the reducer as combiner also, useful in cases of aggregation)
		job.setCombinerClass(FloatSumReducer.class);
		job.setReducerClass(FloatSumReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
