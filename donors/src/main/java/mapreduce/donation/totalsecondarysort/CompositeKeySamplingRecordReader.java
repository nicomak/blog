package mapreduce.donation.totalsecondarysort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mapreduce.donation.secondarysort.CompositeKey;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import data.writable.DonationWritable;

public class CompositeKeySamplingRecordReader extends RecordReader<CompositeKey, NullWritable> {
	
	private SequenceFileRecordReader<Text,DonationWritable> recordReader;
	private List<CompositeKey> keys;
	private int readLimit;
	private int nbSamples;
	private int currentIndex = -1;
	
	public CompositeKeySamplingRecordReader(SequenceFileRecordReader<Text, DonationWritable> recordReader, int readLimit, int nbSamples) {
		
		// Set the underlying core RecordReader
		this.recordReader = recordReader;
		
		// Set configurable values
		this.readLimit = readLimit;
		this.nbSamples = nbSamples;
		
		// Create reservoir of size nbSamples
		this.keys = new ArrayList<>(nbSamples);
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		recordReader.initialize(split, context);
		
		Random random = new Random();
		int inputIndex = 0;
		while (recordReader.nextKeyValue()) {
			
			// If a readLimit was set because we don't want to sample the whole input split, 
			// exit when the limit had been reached
			if (inputIndex > readLimit) {
				break;
			}
			
			if (keys.size() < nbSamples) {
			
				// For the first nbSamples, fill up the reservoir
				DonationWritable donation = recordReader.getCurrentValue();
				CompositeKey compositeKey = getValidCompositeKeyOrNull(donation);
				if (compositeKey != null) {
					keys.add(compositeKey);						
				}
			
			} else {
				
				// When the reservoir is full, add following streaming elements with decreasing probability
				int randomIndex = random.nextInt(inputIndex);
				if (randomIndex < nbSamples) {
					DonationWritable donation = recordReader.getCurrentValue();
					CompositeKey compositeKey = getValidCompositeKeyOrNull(donation);
					if (compositeKey != null) {
						keys.set(randomIndex, compositeKey);						
					}
				}
				
			}
			
			inputIndex++;			
		}	
	}
	
	
	/**
	 * @param donation
	 * @return
	 */
	private CompositeKey getValidCompositeKeyOrNull(DonationWritable donation) {

		// Ignore entries with empty values
		if (StringUtils.isEmpty(donation.donor_state) || StringUtils.isEmpty(donation.donor_city)) {
			return null;
		}
					
		return new CompositeKey(donation.donor_state, donation.donor_city, donation.total);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (currentIndex < keys.size() - 1) {
			currentIndex++;
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public CompositeKey getCurrentKey() {
		return keys.get(currentIndex);
	}
	
	@Override
	public NullWritable getCurrentValue() {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return currentIndex / (float) keys.size();
	}

	@Override
	public void close() throws IOException {
		recordReader.close();
	}
	
}
