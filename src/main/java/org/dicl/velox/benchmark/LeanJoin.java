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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dicl.velox.mapreduce.LeanInputFormat;
import org.dicl.velox.mapreduce.LeanSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class LeanJoin extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(LeanJoin.class);
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		for(int i=0; i<args.length; i++) {
			LOG.info("args["+String.valueOf(i)+"] = " + args[i]); 
		}
		
		String tradeTableDir = args[0];	//table input
		String payTableDir = args[1];	// pay input
		String joinTableDir = args[2]; // output dir
		Job job = Job.getInstance(conf);
		job.setJobName("join");
		job.setJarByClass(LeanJoin.class);

		job.setMapperClass(PreMapper.class);        
		job.setReducerClass(CommonReduce.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		//job.setMapOutputValueClass(Text.class);
		
		job.setGroupingComparatorClass(FirstComparator.class);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	

		job.setInputFormatClass(LeanInputFormat.class);
		//job.setOutputFormatClass(outputFormatClass);

		FileInputFormat.addInputPath(job, new Path(tradeTableDir));
		FileInputFormat.addInputPath(job, new Path(payTableDir));
		//MultipleInputs.setInputPaths(job, new Path(tradeTableDir));
		//MultipleInputs.setInputPaths(job, new Path(payTableDir));
		//MultipleInputs.addInputPath(job, new Path(tradeTableDir), LeanInputFormat.class, PreMapper.class);
		//MultipleInputs.addInputPath(job, new Path(payTableDir), LeanInputFormat.class, PreMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(joinTableDir));
		job.waitForCompletion(true);	

		String zkAddress   = conf.get("velox.recordreader.zk-addr", "172.20.1.40:2381");
		LeanSession session = new LeanSession(zkAddress, job.getStatus().getJobID().toString(), 500000);
		session.deleteChunks();

		session.close();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LeanJoin(), args);
		System.exit(res);
	}
}
