package org.dicl.velox.benchmark;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Step1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> 
{
	private static final Log LOG = LogFactory.getLog(Step1Reducer.class);
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		// use StringBuilder, protect memory leak
		StringBuffer sb = new StringBuffer(); 
		
		//String outlinks = "";
		
		for (IntWritable iw : values)
		{
			//if(first) first = false;
			//else sb.append(",");
			sb.append(",");
			sb.append(Integer.toString(iw.get()));
		}
		//LOG.info("outlinks = " + outlinks);
		//context.write(key, new Text("1.0\t" + outlinks));
		context.write(key, new Text("1.0\t" + sb.toString()));
	}
	
}
