package mapreduce.donation.totalsecondarysort;

import java.io.IOException;

import mapreduce.donation.secondarysort.CompositeKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import data.writable.DonationWritable;

public class CompositeKeySamplingInputFormat extends SequenceFileInputFormat<CompositeKey, NullWritable> {

	public static final String READ_LIMIT_CONFIGKEY = "CompositeKeySamplingInputFormat.readLimit";
	public static final String NB_SAMPLES_CONFIGKEY = "CompositeKeySamplingInputFormat.nbSamples";
	
	@Override
	public RecordReader<CompositeKey, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
	  
		// Create a SequenceFileRecordReader with the real <K,V> types of the input sequence file
		SequenceFileRecordReader<Text, DonationWritable> coreRecordReader = new SequenceFileRecordReader<>();
		
		
		int readLimit = getReadLimit(context.getConfiguration());
		int nbSamples = getNbSamples(context.getConfiguration());
		
		// Create a new SamplingRecordReader, which uses the previous record reader to read <K,V> input types, 
		// take a sample, and transform the sample data into <CompositeKey, NullWritable> return types
		return new CompositeKeySamplingRecordReader(coreRecordReader, readLimit, nbSamples);
	}

	
	public static void setReadLimit(Job job, int nbSamples) {
		job.getConfiguration().setInt(READ_LIMIT_CONFIGKEY, nbSamples);
	}
		
	public static void setNbSamples(Job job, int nbSamples) {
		job.getConfiguration().setInt(NB_SAMPLES_CONFIGKEY, nbSamples);
	}
	
	public static int getReadLimit(Configuration conf) {
		return conf.getInt(READ_LIMIT_CONFIGKEY, Integer.MAX_VALUE);
	}
	
	public static int getNbSamples(Configuration conf) {
		return conf.getInt(READ_LIMIT_CONFIGKEY, 1000);
	}


}
