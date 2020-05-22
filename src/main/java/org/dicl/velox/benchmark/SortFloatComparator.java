package org.dicl.velox.benchmark;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Taken from: https://gist.github.com/geofferyzh/3839714

public class SortFloatComparator extends WritableComparator 
{

	protected SortFloatComparator() {
		super(FloatWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FloatWritable k1 = (FloatWritable)w1;
		FloatWritable k2 = (FloatWritable)w2;
		
		return -1 * k1.compareTo(k2);
	}
	
}
