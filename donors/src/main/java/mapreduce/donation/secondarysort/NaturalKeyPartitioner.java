package mapreduce.donation.secondarysort;

import org.apache.hadoop.mapreduce.Partitioner;

import data.writable.DonationWritable;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, DonationWritable> {

	@Override
	public int getPartition(CompositeKey key, DonationWritable value, int numPartitions) {
		
		// Automatic n-partitioning using hash on the state name
		return Math.abs(key.state.hashCode() * 127) % numPartitions;

		// Commented block below is an example of manual partitioning to 3 reducers
		/*
		if (key.state.compareTo("J") < 0) {
			return 0;
		} else if (key.state.compareTo("R") < 0) {
			return 1;
		} else {
			return 2;
		}
		*/
	}
	
	
	
}
