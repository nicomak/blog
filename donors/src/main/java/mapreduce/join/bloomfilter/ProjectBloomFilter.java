package mapreduce.join.bloomfilter;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import data.writable.ProjectWritable;

public class ProjectBloomFilter {

	public static final Log LOG = LogFactory.getLog(ProjectFilterTest.class);

	/**
	 * Each instance of this mapper outputs a BloomFilter with the data of its own split.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class FilterCreationMapper extends Mapper<Object, ProjectWritable, NullWritable, BloomFilter> {

		private BloomFilter filter = new BloomFilter(2_000_000, 7, Hash.MURMUR_HASH);
		private int counter = 0;

		@Override
		public void map(Object key, ProjectWritable project, Context context) throws IOException, InterruptedException {

			// Add joining key ("project_id" in this example) to bloom filter if the condition is met
			String subject = (project.primary_focus_subject != null) ? project.primary_focus_subject.toLowerCase() : "";
			if (subject.contains("science")) {
				Key filterKey = new Key(project.project_id.getBytes());
				filter.add(filterKey);
				counter++;
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			// Print number of entries in the filter
			LOG.info("Number of entries in BloomFilter : " + counter);

			// Write the filter to HDFS once all maps are finished
			context.write(NullWritable.get(), filter);
		}

	}

	/**
	 * This reducer will merge all BloomFilters from each split into a single BloomFilter.
	 * 
	 * @author Nicomak
	 *
	 */
	public static class FilterMergingReducer extends Reducer<NullWritable, BloomFilter, NullWritable, NullWritable> {

		private static String FILTER_OUTPUT_FILE_CONF = "bloomfilter.output.file";

		private BloomFilter filter = new BloomFilter(2_000_000, 7, Hash.MURMUR_HASH);

		@Override
		protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {

			// Merge all filters by logical OR
			for (BloomFilter value : values) {
				filter.or(value);
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Path outputFilePath = new Path(context.getConfiguration().get(FILTER_OUTPUT_FILE_CONF));
			FileSystem fs = FileSystem.get(context.getConfiguration());

			try (FSDataOutputStream fsdos = fs.create(outputFilePath)) {
				filter.write(fsdos);

			} catch (Exception e) {
				throw new IOException("Error while writing bloom filter to file system.", e);
			}

		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Projects BloomFilter Creation");
		job.setJarByClass(ProjectBloomFilter.class);

		// Input parameters
		Path projectsPath = new Path(args[0]);
		Path filtersFolder = new Path(args[1]);
		String filterOutput = args[1] + Path.SEPARATOR + "filter";

		// Output parameter (sent to Reducer who will write the bloom filter to file system)
		job.getConfiguration().set(FilterMergingReducer.FILTER_OUTPUT_FILE_CONF, filterOutput);

		// Mapper configuration
		job.setMapperClass(FilterCreationMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(BloomFilter.class);

		// Reducer configuration
		job.setReducerClass(FilterMergingReducer.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, projectsPath);
		FileOutputFormat.setOutputPath(job, filtersFolder);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
