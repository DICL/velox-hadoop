package org.dicl.velox.benchmark;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> 
{
	private static final Log LOG = LogFactory.getLog(Step2Mapper.class);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] split = value.toString().split("\t");
		if (split.length < 3)
			return;
		/*
		for(int i=0; i<split.length; i++) {
			LOG.info("split[0]: "  + split[0]);
			LOG.info("split[1]: "  + split[1]);
			LOG.info("split[2]: "  + split[2].substring(0,20));
		}
		 */
		String node = split[0];
		float rank = Float.parseFloat(split[1]);
		
		// If the node doesn't have outlinks
		
		String[] outlinks = split[2].split(",");
		
		float outlinkRank = rank / outlinks.length;
		for (String outlink : outlinks)
		{
			context.write(new Text(outlink), new Text(Float.toString(outlinkRank)));
		}
		
		context.write(new Text(node), new Text("[" + split[2]));
	}
	
}
