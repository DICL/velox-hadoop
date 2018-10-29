package org.dicl.velox.dfs;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class VDFSInputFormat extends InputFormat<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(VDFSInputFormat.class);

  /** 
   * Generate the splits for the logical block
   * @param job the job context
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job, int numSplits) throws IOException {

  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) 
      throws IOException {
      return new RecordReader(split);

  }
}
