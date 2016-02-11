package data.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import data.writable.ProjectWritable;

public class ProjectSerDe extends AbstractSerDe {

	public static final Log LOG = LogFactory.getLog(ProjectSerDe.class.getName());

	private static final int NUM_COLUMNS = 30;
	private List<String> columnNames;
	private StandardStructObjectInspector objInspector;
	private ArrayList<Object> row;

	@Override
	public Object deserialize(Writable obj) throws SerDeException {

		if (!(obj instanceof ProjectWritable)) {
			throw new SerDeException(String.format("Expected type %s but received %s",
					ProjectWritable.class.getName(), obj.getClass().getName()));
		}

		ProjectWritable project = (ProjectWritable) obj;
		row.set(0, project.project_id);
		row.set(1, project.teacher_acctid);
		row.set(2, project.school_id);
		row.set(3, project.school_latitude);
		row.set(4, project.school_longitude);
		row.set(5, project.school_city);
		row.set(6, project.school_state);
		row.set(7, project.school_country);
		row.set(8, project.teacher_prefix);
		row.set(9, project.primary_focus_subject);
		row.set(10, project.primary_focus_area);
		row.set(11, project.secondary_focus_subject);
		row.set(12, project.secondary_focus_area);
		row.set(13, project.resource_type);
		row.set(14, project.poverty_level);
		row.set(15, project.grade_level);
		row.set(16, project.vendor_shipping_charges);
		row.set(17, project.sales_tax);
		row.set(18, project.payment_processing_charges);
		row.set(19, project.fulfillment_labor_materials);
		row.set(20, project.total_price_excluding_opt_donation);
		row.set(21, project.total_price_including_opt_donation);
		row.set(22, project.students_reached);
		row.set(23, project.total_donations);
		row.set(24, project.num_donors);
		row.set(25, project.funding_status);
		row.set(26, project.date_posted);
		row.set(27, project.date_completed);
		row.set(28, project.date_thank_you_packet_mailed);
		row.set(29, project.date_expiration);
		return row;
	}

	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {

		LOG.info("Initialized DonationSerDe");

		String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

		columnNames = Arrays.asList(columnNameProperty.split(","));
		List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

		if (columnNames.size() != columnTypes.size()) {
			throw new SerDeException("Column names and Column types are not of same size.");
		}
		if (columnNames.size() != NUM_COLUMNS) {
			throw new SerDeException("Wrong number of columns received.");
		}

		// Create inspectors for each attribute
		List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(columnNames.size());
		for (int i = 0; i <= 14; i++)
			inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		for (int i = 15; i <= 21; i++)
			inspectors.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
		for (int i = 22; i <= 24; i++)
			inspectors.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		for (int i = 25; i <= 29; i++)
			inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		// StandardStruct uses ArrayList to store the row.
		objInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);

		// Constructing the row object which will be reused for all rows, setting all attributes to null.
		row = new ArrayList<Object>(Collections.nCopies(columnNames.size(), null));
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
		return NullWritable.get();
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return NullWritable.class;
	}
}
