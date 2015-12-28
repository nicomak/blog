package mapreduce.donation.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

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
		out.writeUTF(state);
		out.writeUTF(city);
		out.writeFloat(total);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		state = in.readUTF();
		city = in.readUTF();
		total = in.readFloat();
	}

	@Override
	public int compareTo(CompositeKey o) {
		int stateCmp = state.toLowerCase().compareTo(o.state.toLowerCase());
		if (stateCmp != 0) {
			return stateCmp;
		} else {
			int cityCmp = city.toLowerCase().compareTo(o.city.toLowerCase());
			if (cityCmp != 0) {
				return cityCmp;
			} else {
				return Float.compare(total, o.total);
			}
		}
	}

}
