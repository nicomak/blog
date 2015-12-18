package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import data.tools.StringHelper;

public class DonationWritable implements WritableComparable<DonationWritable> {
	
	public String donation_id;
	public String project_id;
	public String donor_city;
	public String donor_state;
	public String donor_is_teacher;
	public String ddate;
	public float amount;
	public float support;
	public float total;
	public String payment_method;
	public String payment_inc_acct_credit;
	public String payment_inc_campaign_gift_card;
	public String payment_inc_web_gift_card;
	public String payment_promo_matched;
	public String via_giving_page;
	public String for_honoree;
	public String thank_you_packet;
	public String message;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		donation_id = in.readUTF();
		project_id = in.readUTF();
		donor_city = in.readUTF();
		donor_state = in.readUTF();
		donor_is_teacher = in.readUTF();
		ddate = in.readUTF();
		amount = in.readFloat();
		support = in.readFloat();
		total = in.readFloat();
		payment_method = in.readUTF();
		payment_inc_acct_credit = in.readUTF();
		payment_inc_campaign_gift_card = in.readUTF();
		payment_inc_web_gift_card = in.readUTF();
		payment_promo_matched = in.readUTF();
		via_giving_page = in.readUTF();
		for_honoree = in.readUTF();
		thank_you_packet = in.readUTF();
		message = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(donation_id);
		out.writeUTF(project_id);
		out.writeUTF(donor_city);
		out.writeUTF(donor_state);
		out.writeUTF(donor_is_teacher);
		out.writeUTF(ddate);
		out.writeFloat(amount);
		out.writeFloat(support);
		out.writeFloat(total);
		out.writeUTF(payment_method);
		out.writeUTF(payment_inc_acct_credit);
		out.writeUTF(payment_inc_campaign_gift_card);
		out.writeUTF(payment_inc_web_gift_card);
		out.writeUTF(payment_promo_matched);
		out.writeUTF(via_giving_page);
		out.writeUTF(for_honoree);
		out.writeUTF(thank_you_packet);
		out.writeUTF(message);
	}
	
	public void parseLine(String line) throws IOException {
		
		// Remove all double-quotes from line
		line = StringHelper.removeDoubleQuotes(line);
		
		// Split on commas, unless they were escaped with a backslash 
		// e.g. the "message" field can contain commas.
		// Negative limit param "-1" to keep empty values in resulting array
		String[] parts = line.split("(?<!\\\\),", -1);
		
		donation_id = parts[0];
		project_id = parts[1];
		donor_city = parts[4];
		donor_state = parts[5];
		donor_is_teacher = parts[7];
		ddate = parts[8];
		amount = StringHelper.parseFloat(parts[9]);
		support = StringHelper.parseFloat(parts[10]);
		total = StringHelper.parseFloat(parts[11]);
		payment_method = parts[14];
		payment_inc_acct_credit = parts[15];
		payment_inc_campaign_gift_card = parts[16];
		payment_inc_web_gift_card = parts[17];
		payment_promo_matched = parts[18];
		via_giving_page = parts[19];
		for_honoree = parts[20];
		thank_you_packet = parts[21];
		message = parts[22];
	}

	@Override
	public int compareTo(DonationWritable o) {
		return this.donation_id.compareTo(o.donation_id);
	}
	
	@Override
	public String toString() {
		return String.format("%s|project=%s|city=%s|total=%.2f", 
				donation_id, project_id, donor_city, total);
	}

}
