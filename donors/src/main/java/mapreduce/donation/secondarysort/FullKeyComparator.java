package mapreduce.donation.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator {

	public FullKeyComparator() {
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {

		CompositeKey key1 = (CompositeKey) wc1;
		CompositeKey key2 = (CompositeKey) wc2;

		int stateCmp = key1.state.toLowerCase().compareTo(key2.state.toLowerCase());
		if (stateCmp != 0) {
			return stateCmp;
		} else {
			int cityCmp = key1.city.toLowerCase().toLowerCase().compareTo(key2.city.toLowerCase());
			if (cityCmp != 0) {
				return cityCmp;
			} else {
				return -1 * Float.compare(key1.total, key2.total);
			}
		}

	}
}
