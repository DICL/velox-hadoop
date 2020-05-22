package org.dicl.velox.benchmark;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FirstComparator extends WritableComparator{
	private static final Log LOG = LogFactory.getLog(FirstComparator.class);

	public FirstComparator(){
		super(TextPair.class, true);
		LOG.info("first comparator");
	}
	
	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		LOG.info("firstcomparator-compare");
		TextPair o1 = (TextPair) a;
		TextPair o2 = (TextPair) b;
		return o1.getText().compareTo(o2.getText());
	}
}
