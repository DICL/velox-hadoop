package org.dicl.velox.dfs;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
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
  private String path;
  private VeloxDFS vdfs = null;

  public VDFSInputFormat() {
      super();
      vdfs = new VeloxDFS();
  }

  public void addInputPath(String path) {
      this.path = path;
  }

  /** 
   * Generate the splits for the logical block
   * @param job the job context
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
      List<InputSplit> splits = new ArrayList<InputSplit>();

      long fd = vdfs.open(path);
      Metadata md = vdfs.getMetadata(fd, (byte)3);
      for (int i = 0; i < md.numBlock; i++) {
          VDFSInputSplit split = new VDFSInputSplit(md.blocks[i].name, md.blocks[i].host, md.blocks[i].size);
          for (BlockMetadata chunk : md.blocks[i].chunks) {
              split.addChunk(chunk.name, chunk.size, chunk.index);
          }

          splits.add(split);
      }
      vdfs.close(fd);
      return splits;
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
      RecordReader rr = new VDFSRecordReader();
      rr.initialize(split, context);

      return rr;
  }
}
