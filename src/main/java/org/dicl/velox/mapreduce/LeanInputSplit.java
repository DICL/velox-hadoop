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
  public String host;
  public long size;

  /**
   * Constructor.
   */
  public LeanInputSplit() {
    chunks = new ArrayList<Chunk>();
  }

  /**
   * Constructor.
   */
  public LeanInputSplit(String name, String host, long size) {
    chunks = new ArrayList<Chunk>();
    this.logicalBlockName = name;
    this.host = host;
    this.size = size;
  }

  /**
   * adds a chunk.
   */
  public void addChunk(String fname, long size, long seq) {
    chunks.add(new Chunk(fname, size, seq));
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
    host = Text.readString(in);
    size = in.readLong();
    ArrayWritable inChunks = new ArrayWritable(Chunk.class);
    inChunks.readFields(in);
    chunks = new ArrayList<Chunk>(Arrays.asList((Chunk[])inChunks.toArray()));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, logicalBlockName);
    Text.writeString(out, host);
    out.writeLong(size);
    ArrayWritable inChunks = new ArrayWritable(Chunk.class,
        chunks.toArray(new Chunk[chunks.size()]));
    inChunks.write(out);
  }
}
