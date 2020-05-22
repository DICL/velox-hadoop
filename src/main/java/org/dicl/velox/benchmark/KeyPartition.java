package org.dicl.velox.benchmark;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class KeyPartition extends Partitioner<TextPair, Text>{
	private static final Log LOG = LogFactory.getLog(KeyPartition.class);

	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		LOG.info("key partition");
		return (key.getText().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}


