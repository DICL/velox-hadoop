package org.dicl.velox.benchmark;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class Step1Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{
	private static final Log LOG = LogFactory.getLog(Step1Mapper.class);

	private static int nodesCount;
	private static int currentNode;
	private static int edgesCount;
	private static int currentEdge;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		// NodesCount(N) EdgesCount(M)
		//if(key.get() != 0 || value.toString() != "") {
			//LOG.info("key: "  + key.toString());
			//LOG.info("value: " + value.toString());
			//String[] split = value.toString().split("\\s");
			//int nodenum1 = Integer.parseInt(split[0]);
			//int nodenum2 = Integer.parseInt(split[1]);
			//context.write(new IntWritable(nodenum1), new IntWritable(nodenum2));
			String[] split = value.toString().split("\\s");
			if(split.length != 2) return;
			context.write(new IntWritable(Integer.parseInt(split[0])),
				new IntWritable(Integer.parseInt(split[1])));
		//currentEdge++;
		/*}
		else {
			return;
		}*/


	/*	
		if (key.get() == 0) 
		{
			//String[] split = value.toString().split(" ");
			String[] split = value.toString().split("\\s");

			nodesCount = Integer.parseInt(split[0]);
			edgesCount = Integer.parseInt(split[1]);
		}
		else
		{
			// N nodes
			if (currentNode < nodesCount)
			{
				currentNode++;
				return;
			}
			// M edges
			else if (currentEdge < edgesCount)
			{
				//String[] split = value.toString().split(" ");
				String[] split = value.toString().split("\\s");
				context.write(new IntWritable(Integer.parseInt(split[0])), new IntWritable(Integer.parseInt(split[1])));
				currentEdge++;
			}
		}*/
		
	}
	
}
