package org.dicl.velox.benchmark;
import java.io.*;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Step3Mapper extends Mapper<LongWritable, Text, FloatWritable, Text> 
{
	
	private HashMap<String, String> urls;
	private static final Log LOG = LogFactory.getLog(Step3Mapper.class);
	
	@Override
	protected void setup(Context context) throws IOException
	{
		/*
		urls = new HashMap<String, String>();
		
		Path path = new Path(context.getConfiguration().get("urls_path"));
		LOG.info("path = " + path.toString());		// input/"filename"
		Configuration conf = new Configuration();
		conf.addResource(new Path("/home/velox/hadoop-etc/core-site.xml"));
		conf.addResource(new Path("/home/velox/hadoop-etc/hdfs-site.xml"));
		conf.addResource(new Path("/home/velox/hadoop-etc/mapred-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		
		boolean firstLine = true;
		int currentNode = 0;
		int nodesCount = 0;*/

		String line;
		/*
		while ((line = br.readLine()) != null)
		{
			LOG.info("while");
			//String[] split = line.split(" ");
			String[] split = line.split("\\s+");
			LOG.info("split2.length = " + String.valueOf(split.length));
			for(int i=0; i<split.length; i++) {
				LOG.info("split2["+String.valueOf(i)+"] = " + split[i]);	
			}

			if (firstLine)
			{
				LOG.info("first line");
				nodesCount = Integer.parseInt(split[0]);
				LOG.info("nodesCount = " + String.valueOf(nodesCount));
				firstLine = false;
			}
			else
			{
				if (currentNode < nodesCount)
				{
					LOG.info("else-if");
					LOG.info("split2[0] = " + split[0]);
					LOG.info("split2[1] = " + split[1]);
					urls.put(split[0], split[1]);
					currentNode++;
				}
				else 
				{
					LOG.info("break");
					break;
				}
			}
		}*/
		
		//br.close();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		//LOG.info("value = " + value.toString());
		//String[] split = value.toString().split("\t");
		String[] split = value.toString().split("\\s+");
		//LOG.info("split.length = " + String.valueOf(split.length));
		//for(int i=0; i<split.length; i++) {
		//	LOG.info("split["+String.valueOf(i)+"] = " + split[i]);
		//}
		float rank = Float.parseFloat(split[1]);
		//String url = urls.get(split[0]);
		String url = split[0];
		//LOG.info("url = " + url);
		//LOG.info("rank = " + String.valueOf(rank));

		context.write(new FloatWritable(rank), new Text(url));
	}
	
}
