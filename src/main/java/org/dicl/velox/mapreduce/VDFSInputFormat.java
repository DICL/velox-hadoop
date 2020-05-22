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
  private int task_id;

  public static enum Counter {
      BYTES_READ
  }

  public VDFSInputFormat() {
      super();
      vdfs = new VeloxDFS(null,0,true);
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
	  LOG.info("Input File : " + path);
      long fd = vdfs.open(path);
      Metadata md = vdfs.getMetadata(fd, (byte)3);
		LOG.info("md.numblock = " + String.valueOf(md.numBlock));

      // Generate the splits per each generated logical block
      List<InputSplit> splits = new ArrayList<InputSplit>();
      for (int i = 0; i < md.numBlock; i++) {

          VDFSInputSplit split = new VDFSInputSplit(md.blocks[i].name, job.getJobID().toString(), task_id);
         /* for (BlockMetadata chunk : md.blocks[i].chunks) {
              split.addChunk(chunk.HBname, chunk.size, chunk.index, chunk.offset, chunk.host);
          }*/
          splits.add(split);
        //  LOG.info("P: " + md.blocks[i].name + " len: "  + md.blocks[i].size + " host: "  + md.blocks[i].host + "numChunks: ");
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
