package org.dicl.velox.benchmark;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dicl.velox.mapreduce.LeanSession;
import org.dicl.velox.mapreduce.LeanInputFormat;




public class PageRank extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(PageRank.class);	
	// args example = ["/input", "/output", "/input/pagerank_data.txt", "0.85", "5", "0.01", "true", "true"]
	private PageRank() {}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}
	//public static void main(String[] args) throws Exception
	public int run(String[] args) throws Exception
	{
		if (args.length != 8)
		{
			System.out.println("Invalid arguments, expected 7 (inputpath, outputpath, datapath, df, maxruns, deleteoutput, showresults).");
			System.exit(1);
		}

		for(int i=0; i<args.length; i++)
			System.out.println("args["+String.valueOf(i)+"] = " + args[i]);

		int maxRuns = Integer.parseInt(args[4]);
		float dampingFactor = Float.parseFloat(args[3]);
		float mindiff = Float.parseFloat(args[5]);
		FileSystem fs = FileSystem.get(new Configuration());
			
		// Deleting the output folder if asked/needed
		if (Boolean.parseBoolean(args[6]))
		{
			Path outputPath = new Path(args[1]);
			if (fs.exists(outputPath))
			{
				System.out.println("Deleting /output..");
				fs.delete(outputPath, true);
			}
		}
		
		// Step 1
		boolean success = step1(args[0], args[1] + "/ranks0");
		
		
		// Step 2
		HashMap<Integer, Float> lastRanks = getRanks(fs, args[1] + "/ranks0");
		
		for (int i = 0; i < maxRuns; i++) 
		{
			success = success && step2(args[1] + "/ranks" + i, args[1] + "/ranks" + (i + 1), dampingFactor);
			
			// Calculate diff of ranks
			HashMap<Integer, Float> newRanks = getRanks(fs, args[1] + "/ranks" + (i + 1));
			float diff = calculateDiff(lastRanks, newRanks);
			System.out.println("Run #" + (i + 1) + " finished (score diff: " + diff + ").");
			
			// If the diff is lower than the mindiff we stop the iterations
			if (diff < mindiff)
				break;
			// Otherwise continue
			else
				lastRanks = newRanks;
		}
		
		
		// Step 3
		success = success && step3(args[1] + "/ranks" + maxRuns, args[1] + "/ranking", args[2]);
		
		// Show results if asked
		if (Boolean.parseBoolean(args[7]))
		{
			showResults(fs, args[1] + "/ranking");
		}
		System.exit(success ? 0 : 1);
		
		return 0;
	}
	
	//private static boolean step1(String input, String output) throws Exception
	private boolean step1(String input, String output) throws Exception
	{
		//Configuration conf = new Configuration();
		Configuration conf = getConf();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

		System.out.println("Step 1..");
		Job job = Job.getInstance(conf, "Step 1");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(Step1Mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(Step1Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
	
		job.setInputFormatClass(LeanInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);

		//String zkAddress = conf.get("velox.recordreader.zk-addr", "172.20.1.80:2381");
		//LeanSession session = new LeanSession(zkAddress, job.getStatus().getJobID().toString(), 500000);
		//session.deleteChunks();
		//session.close();
		
		return true;
	}
	
	private static boolean step2(String input, String output, float dampingFactor) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		conf.setFloat("df", dampingFactor);
		
		System.out.println("Step 2..");
		Job job = Job.getInstance(conf, "Step 2");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(Step2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setReducerClass(Step2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}

	private static boolean step3(String input, String output, String urlsPath) throws Exception
	{
		Configuration conf = new Configuration();
		System.out.println("input = " + input);
		System.out.println("output = " + output);
		System.out.println("urlsPath = " + urlsPath);
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		conf.set("urls_path", urlsPath);
		
		System.out.println("Step 3..");
		Job job = Job.getInstance(conf, "Step 3");
		job.setJarByClass(PageRank.class);
		job.setSortComparatorClass(SortFloatComparator.class);
		
		job.setMapperClass(Step3Mapper.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	private static void showResults(FileSystem fs, String dir) throws Exception
	{
		Path path = new Path(dir + "/part-r-00000");
		if (!fs.exists(path))
		{
			System.out.println("The file part-r-00000 doesn't exist.");
			return;
		}
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line;
		while ((line = br.readLine()) != null)
		{
			System.out.println(line);
		}
	}
	
	private static HashMap<Integer, Float> getRanks(FileSystem fs, String dir) throws Exception
	{
		Path path = new Path(dir + "/part-r-00000");
		if (!fs.exists(path))
			throw new Exception("The file part-r-00000 doesn't exist.");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line;
		HashMap<Integer, Float> ranks = new HashMap<Integer, Float>();
		
		while ((line = br.readLine()) != null)
		{
			//System.out.println(line);
			String[] split = line.split("\t");
			if(split[0].length() * split[1].length() == 0) continue;
			//System.out.println(split[0] + "\t" + split[1]);
			ranks.put(Integer.parseInt(split[0]), Float.parseFloat(split[1]));
		}
		
		return ranks;
	}
	
	private static float calculateDiff(HashMap<Integer, Float> lastRanks, HashMap<Integer, Float> newRanks)
	{
		float diff = 0;
		
		for (int key : newRanks.keySet())
		{
			float lri = lastRanks.containsKey(key) ? lastRanks.get(key) : 0;
			diff += Math.abs(newRanks.get(key) - lri);
		}
		
		return diff;
	}
	
}
