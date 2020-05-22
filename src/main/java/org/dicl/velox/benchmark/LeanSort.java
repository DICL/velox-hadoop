/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dicl.velox.benchmark;



import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dicl.velox.mapreduce.LeanInputFormat;
import org.dicl.velox.mapreduce.LeanSession;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.*;


public class LeanSort<K,V> extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(LeanSort.class);
	public static final String REDUCES_PER_HOST = 
		"mapreduce.sort.reducesperhost";
	private Job job = null;
	/*
	static int printUsage() {
		System.out.println("leansort [-r <reduces>] " +
				"[-inFormat <input format class>] " +
				"[-outFormat <output format class>] " + 
				"[-outKey <output key class>] " +
				"[-outValue <output value class>] " +
				"[-totalOrder <pcnt> <num samples> <max splits>] " +
				"<input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return 2;
	}*/


	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private static IntWritable data = new IntWritable();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//if(key.get() != 0) {
				String line = value.toString();
				if(line.equals("")) return;
				data.set(Integer.parseInt(line));
				//LOG.info("data = " + data.toString());
				context.write(data, new IntWritable(1));
			//}
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static IntWritable linenum = new IntWritable(1);

		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(linenum, key);
			}	
			linenum = new IntWritable(linenum.get() + 1);
		}
	}

	public static class Patition extends Partitioner<IntWritable, IntWritable>{
		public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
			int maxNum = 10000;
			int bound = maxNum / numPartitions + 1;
			int keynum = key.get();
			for(int i=0; i< numPartitions;i++){
				if(keynum< bound * i && keynum >= bound * (i-1)) {
					return i-1;
				}
			}
			return -1;
		}
	}

	/**
	 * The main driver for sort program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the 
	 *                     job tracker.
	 */
	public int run(String[] args) throws Exception {

		for(int i=0; i<args.length; i++) {
			LOG.info("args[" + String.valueOf(i) + "] = " + args[i]);
		}
		Configuration conf = getConf();

		Job job = new Job(conf, "sort");
		job.setJarByClass(LeanSort.class);
		job.setNumReduceTasks(160);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		//job.setPartitionerClass(Patition.class);
		job.setInputFormatClass(LeanInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		String zkAddress   = conf.get("velox.recordreader.zk-addr", "172.20.1.80:2381");
		LeanSession session = new LeanSession(zkAddress, job.getStatus().getJobID().toString(), 500000);
		session.deleteChunks();
		session.close();

		return 0;

	}



	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LeanSort(), args);
		System.exit(res);
	}

	/**
	 * Get the last job that was run using this instance.
	 * @return the results of the last job that was run
	 */
	public Job getResult() {
		return job;
	}
}
