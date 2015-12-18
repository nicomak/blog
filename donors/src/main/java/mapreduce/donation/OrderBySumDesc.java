package mapreduce.donation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderBySumDesc {

	public static class InverseCitySumMapper extends Mapper<Text, Text, FloatWritable, Text> {

		private FloatWritable floatSum = new FloatWritable();		
		
		@Override
		public void map(Text city, Text sum, Context context) throws IOException, InterruptedException {
			float floatVal = Float.parseFloat(sum.toString());
			floatSum.set(floatVal);
			context.write(floatSum, city);
		}
	}
	
	public static class DescendingFloatComparator extends WritableComparator {
		
	    public DescendingFloatComparator() {
	        super(FloatWritable.class, true);
	    }

	    @SuppressWarnings("rawtypes")
		@Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	FloatWritable key1 = (FloatWritable) w1;
	    	FloatWritable key2 = (FloatWritable) w2;          
	        return -1 * key1.compareTo(key2);
	    }
	}
	
	public static void main(String[] args) throws Exception {
		
		Job job = Job.getInstance(new Configuration(), "Order By Sum Desc");
		job.setJarByClass(DonationsSumByCity.class);
		
		// The mapper which transforms (K:V) => (float(V):K)
		job.setMapperClass(InverseCitySumMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		// Sort with descending float order
		job.setSortComparatorClass(DescendingFloatComparator.class);
		
		// Use default Reducer which simply transforms (K:V1,V2) => (K:V1), (K:V2)
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1);
		
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
