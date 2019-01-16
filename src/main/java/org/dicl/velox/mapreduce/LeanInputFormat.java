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

public class LeanInputFormat extends InputFormat<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(LeanInputFormat.class);

  public static enum Counter {
    BYTES_READ
  }

  /** 
   * Generate the splits for the logical block (This function is called at the MRapp master).
   * @param job the job context
   * @throws IOException if fails to open the given input file
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    LOG.info("LeanInputFormat Entered");
    VeloxDFS vdfs = new VeloxDFS();

    // Setup Zookeeper ZNODES
    String zkAddress   = job.getConfiguration().get("velox.recordreader.zk-addr", 
        "192.168.0.101:2181");
    LeanSession session = new LeanSession(zkAddress, job.getJobID().toString(), 500000);
    session.setupZk();
    session.close();

    // Generate Logical Block distribution
    Configuration conf = job.getConfiguration();
    String filePath = conf.get("velox.inputfile");
    long fd = vdfs.open(filePath);
    Metadata md = vdfs.getMetadata(fd, (byte)3);

    // Set important variables 
    conf.set("velox.numChunks", String.valueOf(md.numChunks));
    conf.set("velox.numStaticChunks", String.valueOf(md.numStaticBlocks));

    // Generate the splits per each generated logical block
    List<InputSplit> splits = new ArrayList<InputSplit>();
    int totalChunks = 0;
    for (int i = 0; i < md.numBlock; i++) {
      LeanInputSplit split = new LeanInputSplit(md.blocks[i].name, md.blocks[i].host, 
          md.blocks[i].size);

      for (BlockMetadata chunk : md.blocks[i].chunks) {
        split.addChunk(chunk.name, chunk.size, chunk.index);
        totalChunks++;
      }
      splits.add(split);
      LOG.info("P: " + md.blocks[i].name + " len: "  + md.blocks[i].size
          + " host: "  + md.blocks[i].host + " numChunks: " + md.blocks[i].chunks.length);
    }

    LOG.info("Total number of chunks " + md.numChunks);
    vdfs.close(fd);
    return splits;
  }

  /** 
   * Creates a LeanRecordReader from the split (this function is called remotely).
   * @param split the split to be processed
   * @param context the task context
   * @throws IOException if fails to create a record reader
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new LeanRecordReader();
  }
}
