package org.dicl.velox.mapreduce;

import com.dicl.velox.VeloxDFS;
import com.dicl.velox.model.BlockMetadata;
import com.dicl.velox.model.Metadata;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.net.*;

public class LeanInputFormat extends InputFormat<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(LeanInputFormat.class);
//	private static final long MinSplitSize = 134217728l; // 128MB

	public static enum Counter {
		BYTES_READ
	}

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		int task_id = job.getJobID().hashCode();
		if(task_id < 0){
			task_id *= -1;
		}
		task_id %= 1000;

		VeloxDFS vdfs = new VeloxDFS(job.getJobID().toString(), task_id, true);

		// Setup Zookeeper ZNODES
		String zkAddress   = job.getConfiguration().get("velox.recordreader.zk-addr", "172.20.1.80:2381");

		LOG.info("zkAddress: " + zkAddress + " " + job.getJobID().toString());
		LeanSession session = new LeanSession(zkAddress, job.getJobID().toString(), 500000);
		session.setupZk();
		session.close();

		// Generate Logical Block distribution
		Configuration conf = job.getConfiguration();
		String filePath = conf.get("velox.inputfile");
		String jobID = job.getJobID().toString();

		long fd = vdfs.open(filePath);
		Metadata md = vdfs.getMetadata(fd, (byte)3);
		long slotNum = Integer.parseInt(job.getConfiguration().get("mapreduce.task.slot"));
		long MinSplitSize = Integer.parseInt(job.getConfiguration().get("velox.recordreader.buffersize")); 
		// Set important variables 
		// Generate the splits per each generated logical block
		List<InputSplit> splits = new ArrayList<InputSplit>();

		for (int i = 0; i < md.numBlock; i++) { // md.numBlock => md.numSlot
			//long sNum = md.blocks[i].size % MinSplitSize == 0 ? md.blocks[i].size / MinSplitSize : md.blocks[i].size / MinSplitSize + 1;  
			long sNum = md.blocks[i].size / MinSplitSize + 1 + 15;  
			// For multiwaves
			sNum = sNum > slotNum ? slotNum : sNum; // disable multiple wave
			//long sNum = slotNum;
			LOG.info("SplitSize : " + md.blocks[i].size + " SlotNum : " + sNum + " host: " + md.blocks[i].host);
			for(long j = 0; j < sNum; j++){
				LeanInputSplit split = new LeanInputSplit(md.blocks[i].name, md.blocks[i].host, jobID, task_id);
				splits.add(split);
			}
		}

		LOG.info("Total number of chunks " + md.numChunks);
		vdfs.close(fd);
		return splits;
	}

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new LeanRecordReader();
	}
}
