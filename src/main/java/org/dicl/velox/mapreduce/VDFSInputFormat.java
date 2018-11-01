package org.dicl.velox.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.dicl.velox.VeloxDFS;
import com.dicl.velox.model.Metadata;
import com.dicl.velox.model.BlockMetadata;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.lang.InterruptedException;

public class VDFSInputFormat extends InputFormat<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(VDFSInputFormat.class);
  private VeloxDFS vdfs = null;

  public static enum Counter {
      BYTES_READ
  }

  public VDFSInputFormat() {
      super();
      vdfs = new VeloxDFS();
  }

  /** 
   * Generate the splits for the logical block (This function is called at the MRapp master)
   * @param job the job context
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
      LOG.info("VDFSInputFormat Entered");

      // Generate Logical Block distribution
      String path = job.getConfiguration().get("vdfsInputFile");
      long fd = vdfs.open(path);
      Metadata md = vdfs.getMetadata(fd, (byte)3);

      // Generate the splits per each generated logical block
      List<InputSplit> splits = new ArrayList<InputSplit>();
      for (int i = 0; i < md.numBlock; i++) {
          VDFSInputSplit split = new VDFSInputSplit(md.blocks[i].name, md.blocks[i].host, md.blocks[i].size);
          for (BlockMetadata chunk : md.blocks[i].chunks) {
              split.addChunk(chunk.name, chunk.size, chunk.index);
          }
          splits.add(split);
          LOG.info("P: " + md.blocks[i].name + " len: "  + md.blocks[i].size + " host: "  + md.blocks[i].host);
      }
      vdfs.close(fd);
      return splits;
  }

  /** 
   * Creates a VDFSRecordReader from the split (this function is called remotely)
   * @param split the split to be processed
   * @param context the task context
   * @throws IOException
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
      return new VDFSRecordReader();
  }
}
