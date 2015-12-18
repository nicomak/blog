package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import data.tools.StringHelper;

public class ProjectWritable implements WritableComparable<ProjectWritable> {
	
	public String project_id;
	public String teacher_acctid;
	public String school_id;
	public String school_latitude;
	public String school_longitude;
	public String school_city;
	public String school_state;
	public String school_country;
	public String teacher_prefix;
	public String primary_focus_subject;
	public String primary_focus_area;
	public String secondary_focus_subject;
	public String secondary_focus_area;
	public String resource_type;
	public String poverty_level;
	public String grade_level;
	public float vendor_shipping_charges;
	public float sales_tax;
	public float payment_processing_charges;
	public float fulfillment_labor_materials;
	public float total_price_excluding_opt_donation;
	public float total_price_including_opt_donation;
	public int students_reached;
	public int total_donations;
	public int num_donors;
	public String funding_status;
	public String date_posted;
	public String date_completed;
	public String date_thank_you_packet_mailed;
	public String date_expiration;	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		project_id = in.readUTF();
		teacher_acctid = in.readUTF();
		school_id = in.readUTF();
		school_latitude = in.readUTF();
		school_longitude = in.readUTF();
		school_city = in.readUTF();
		school_state = in.readUTF();
		school_country = in.readUTF();
		teacher_prefix = in.readUTF();
		primary_focus_subject = in.readUTF();
		primary_focus_area = in.readUTF();
		secondary_focus_subject = in.readUTF();
		resource_type = in.readUTF();
		poverty_level = in.readUTF();
		secondary_focus_area = in.readUTF();
		grade_level = in.readUTF();	 
		
		vendor_shipping_charges = in.readFloat();
		sales_tax = in.readFloat();
		payment_processing_charges = in.readFloat();
		fulfillment_labor_materials = in.readFloat();
		total_price_excluding_opt_donation = in.readFloat();
		total_price_including_opt_donation = in.readFloat();
		students_reached = in.readInt();
		total_donations = in.readInt();
		num_donors = in.readInt();
		 
		funding_status = in.readUTF();
		date_posted = in.readUTF();
		date_completed = in.readUTF();
		date_thank_you_packet_mailed = in.readUTF();
		date_expiration = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(project_id);
		out.writeUTF(teacher_acctid);
		out.writeUTF(school_id);
		out.writeUTF(school_latitude);
		out.writeUTF(school_longitude);
		out.writeUTF(school_city);
		out.writeUTF(school_state);
		out.writeUTF(school_country);
		out.writeUTF(teacher_prefix);
		out.writeUTF(primary_focus_subject);
		out.writeUTF(primary_focus_area);
		out.writeUTF(secondary_focus_subject);
		out.writeUTF(resource_type);
		out.writeUTF(poverty_level);
		out.writeUTF(secondary_focus_area);
		out.writeUTF(grade_level);
		
		out.writeFloat(vendor_shipping_charges);
		out.writeFloat(sales_tax);
		out.writeFloat(payment_processing_charges);
		out.writeFloat(fulfillment_labor_materials);
		out.writeFloat(total_price_excluding_opt_donation);
		out.writeFloat(total_price_including_opt_donation);
		
		out.writeInt(students_reached);
		out.writeInt(total_donations);
		out.writeInt(num_donors);
		
		out.writeUTF(funding_status);
		out.writeUTF(date_posted);
		out.writeUTF(date_completed);
		out.writeUTF(date_thank_you_packet_mailed);
		out.writeUTF(date_expiration);
	}
	
	public void parseLine(String line) throws IOException {
		
		// Remove all double-quotes from line
		line = StringHelper.removeDoubleQuotes(line);
		
		// Split on commas, unless they were escaped with a backslash
		// Negative limit param "-1" to keep empty values in resulting array
		String[] parts = line.split("(?<!\\\\),", -1);
		
		project_id = parts[0];
		teacher_acctid = parts[1];
		school_id = parts[2];
		school_latitude = parts[4];
		school_longitude = parts[5];
		school_city = parts[6];
		school_state = parts[7];
		school_country = parts[11];
		teacher_prefix = parts[18];
		primary_focus_subject = parts[21];
		primary_focus_area = parts[22];
		secondary_focus_subject = parts[23];
		secondary_focus_area = parts[24];
		resource_type = parts[25];
		poverty_level = parts[26];
		grade_level = parts[27];
		
		vendor_shipping_charges = StringHelper.parseFloat(parts[28]);
		sales_tax = StringHelper.parseFloat(parts[29]);
		payment_processing_charges = StringHelper.parseFloat(parts[30]);
		fulfillment_labor_materials = StringHelper.parseFloat(parts[31]);
		total_price_excluding_opt_donation = StringHelper.parseFloat(parts[32]);
		total_price_including_opt_donation = StringHelper.parseFloat(parts[33]);
		
		students_reached = StringHelper.parseInt(parts[34]);
		total_donations = StringHelper.parseInt(parts[35]);
		num_donors = StringHelper.parseInt(parts[36]);
		
		funding_status = parts[39];
		date_posted = parts[40];
		date_completed = parts[41];
		date_thank_you_packet_mailed = parts[42];
		date_expiration = parts[43];
	}

	@Override
	public int compareTo(ProjectWritable o) {
		return this.project_id.compareTo(o.project_id);
	}
	
	@Override
	public String toString() {
		return String.format("%s|school=%s|total_amount=%s|status=%s", 
				project_id, school_id, total_price_including_opt_donation, funding_status);
	}

}
