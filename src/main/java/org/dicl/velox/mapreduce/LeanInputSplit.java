package org.dicl.velox.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Object;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class LeanInputSplit extends InputSplit implements Writable {
  public ArrayList<Chunk> chunks;
  public String logicalBlockName;
  public String jobID;
  public String host;
  public long size;
  public long taskID;
  /**
   * Constructor.
   */
  public LeanInputSplit() {
    chunks = new ArrayList<Chunk>();
  }

  /**
   * Constructor.
   */
  public LeanInputSplit(String name, String host, String jobID, int taskID) {
    chunks = new ArrayList<Chunk>();
    this.logicalBlockName = name;
	this.host = host;
	this.jobID = jobID;
	this.taskID = taskID;
  }

   //adds a chunk.
  public void addChunk() {
    chunks.add(new Chunk());
  }

  @Override
  public String[] getLocations() throws IOException {
    String[] s = new String[1]; 
    s[0] = host;
    return s;
  }

  @Override
  public long getLength() throws IOException {
    return chunks.stream().mapToLong(o -> ((Chunk)o).size).sum(); // YAY Functional
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    logicalBlockName = Text.readString(in);
    jobID = Text.readString(in);
    host = Text.readString(in);
	taskID = in.readLong();
    ArrayWritable inChunks = new ArrayWritable(Chunk.class);
    inChunks.readFields(in);
    chunks = new ArrayList<Chunk>(Arrays.asList((Chunk[])inChunks.toArray()));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, logicalBlockName);
    Text.writeString(out, jobID);
    Text.writeString(out, host);
    out.writeLong(taskID);
    ArrayWritable inChunks = new ArrayWritable(Chunk.class,
        chunks.toArray(new Chunk[chunks.size()]));
    inChunks.write(out);
  }
}
