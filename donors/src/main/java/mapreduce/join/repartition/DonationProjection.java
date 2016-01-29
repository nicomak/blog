package mapreduce.join.repartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import data.writable.DonationWritable;

/**
 * Writable class used for containing only the necessary field values for "donation" data.
 * 
 * @author Nicomak
 *
 */
public class DonationProjection implements WritableComparable<DonationProjection> {

	public String donation_id;
	public String project_id;
	public String ddate;
	public String donor_city;
	public float total;

	public static DonationProjection makeProjection(DonationWritable donation) {
		DonationProjection dp = new DonationProjection();
		dp.donation_id = donation.donation_id;
		dp.project_id = donation.project_id;
		dp.ddate = donation.ddate;
		dp.donor_city = donation.donor_city;
		dp.total = donation.total;
		return dp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, donation_id);
		WritableUtils.writeString(out, project_id);
		WritableUtils.writeString(out, ddate);
		WritableUtils.writeString(out, donor_city);
		out.writeFloat(total);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		donation_id = WritableUtils.readString(in);
		project_id = WritableUtils.readString(in);
		ddate = WritableUtils.readString(in);
		donor_city = WritableUtils.readString(in);
		total = in.readFloat();
	}

	@Override
	public int compareTo(DonationProjection o) {
		return donation_id.compareTo(o.donation_id);
	}
}