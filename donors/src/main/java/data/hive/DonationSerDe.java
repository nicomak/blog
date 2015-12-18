package data.hive;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import data.writable.DonationWritable;

public class DonationSerDe extends AbstractSerDe {

	public static final Log LOG = LogFactory.getLog(DonationSerDe.class.getName());
	
	private static final int NUM_COLUMNS = 18;
	private List<String> columnNames;
	private StandardStructObjectInspector objInspector;
	private ArrayList<Object> object;

	@Override
	public Object deserialize(Writable obj) throws SerDeException {
		
		if (!(obj instanceof DonationWritable)) {
			throw new SerDeException(String.format("Expected type %s but received %s",
					DonationWritable.class.getName(), obj.getClass().getName()));
		}
		
		DonationWritable donation = (DonationWritable) obj;
		object.set(0, donation.donation_id);
		object.set(1, donation.project_id);
		object.set(2, donation.donor_city);
		object.set(3, donation.donor_state);
		object.set(4, donation.donor_is_teacher);
		object.set(5, donation.ddate);
		object.set(6, donation.amount);
		object.set(7, donation.support);
		object.set(8, donation.total);
		object.set(9, donation.payment_method);
		object.set(10, donation.payment_inc_acct_credit);
		object.set(11, donation.payment_inc_campaign_gift_card);
		object.set(12, donation.payment_inc_web_gift_card);
		object.set(13, donation.payment_promo_matched);
		object.set(14, donation.via_giving_page);
		object.set(15, donation.for_honoree);
		object.set(16, donation.thank_you_packet);
		object.set(17, donation.message);
		return object;
	}

	@Override
	public void initialize(Configuration job, Properties table) throws SerDeException {
		
		LOG.info("Initialized");
		
		String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
	    String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);

	    columnNames = Arrays.asList(columnNameProperty.split(","));
	    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

	    if (columnNames.size() != columnTypes.size()) {
	    	throw new SerDeException("Column names and Column types are not of same size.");
	    }
	    if (columnNames.size() != NUM_COLUMNS) {
	    	throw new SerDeException("Wrong number of columns received.");
	    }

	    // Check that the 3 donation amounts are still float values
	    checkType(columnTypes, 6, TypeInfoFactory.floatTypeInfo);
	    checkType(columnTypes, 7, TypeInfoFactory.floatTypeInfo);
	    checkType(columnTypes, 8, TypeInfoFactory.floatTypeInfo);

	    // Create inspectors for each attribute
	    List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(columnNames.size());
	    for (int i = 0; i < 6; i++)
	    	inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    for (int i = 0; i < 3; i++)
	    	inspectors.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
	    for (int i = 0; i < 9; i++)
	    	inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

	    // StandardStruct uses ArrayList to store the row.
	    objInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);

	    // Constructing the row object, etc, which will be reused for all rows.
	    object = new ArrayList<Object>(columnNames.size());
	    
	    // TODO: check if can remove
	    for (String columnName : columnNames) {
	    	object.add(null);
	    }

	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
		return NullWritable.get();
	}

	public void checkType(List<TypeInfo> columnTypes, int idx, TypeInfo expectedType) throws SerDeException {
		if (!columnTypes.get(idx).equals(expectedType)) {
			String errorMsg = String.format(
					"Expecting type %s but received type %s at index %d (%s)",
					expectedType, columnTypes.get(idx), idx, columnNames.get(idx));
			throw new SerDeException(errorMsg);
		}
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return  objInspector;
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
