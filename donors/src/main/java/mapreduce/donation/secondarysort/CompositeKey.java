package mapreduce.donation.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Composite key formed by a Natural Key (state) and Secondary Keys (city, total).
 * 
 * @author Nicomak
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

	public String state;
	public String city;
	public float total;
	
	public CompositeKey() {
	}
	
	public CompositeKey(String state, String city, float total) {
		super();
		this.set(state, city, total);
	}
	
	public void set(String state, String city, float total) {
		this.state = (state == null) ? "" : state;
		this.city = (city == null) ? "" : city;
		this.total = total;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, state);
		WritableUtils.writeString(out, city);
		out.writeFloat(total);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		state = WritableUtils.readString(in);
		city = WritableUtils.readString(in);
		total = in.readFloat();
	}

	@Override
	public int compareTo(CompositeKey o) {
		int stateCmp = state.compareTo(o.state);
		if (stateCmp != 0) {
			return stateCmp;
		} else {
			int cityCmp = city.compareTo(o.city);
			if (cityCmp != 0) {
				return cityCmp;
			} else {
				return Float.compare(total, o.total);
			}
		}
	}
	
}
