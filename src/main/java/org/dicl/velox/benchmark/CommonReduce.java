package org.dicl.velox.benchmark;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.dicl.velox.mapreduce.LeanInputFormat;
import org.dicl.velox.mapreduce.LeanSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CommonReduce extends Reducer<TextPair, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(CommonReduce.class);	
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		LOG.info("Common Reducd class");
		String tradeId = values.iterator().next().toString(); //first value is tradeID
		System.out.println(tradeId);
		while(values.iterator().hasNext()){
			//next is payID
			String payID = values.iterator().next().toString();
			context.write(new Text(tradeId), new Text(payID));
		}
	}

}
