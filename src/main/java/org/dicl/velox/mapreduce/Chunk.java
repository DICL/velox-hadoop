package org.dicl.velox.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Chunk implements Writable {
  public String fileName;
  public long size;
  //public long index;
  public int index;
	public long offset;
	public String host;

  public Chunk()  { }

  /**
   * Constructor.
   */
  //public Chunk(String fileName, long size, long index, long offset, String host)  {
  public Chunk(String fileName, long size, int index, long offset, String host)  {
    this.fileName = fileName;
    this.index = index;
    this.size = size;
		this.offset = offset;
		this.host = host;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fileName = Text.readString(in);
    size = in.readLong();
    //index = in.readLong();
    index = in.readInt();
		offset = in.readLong();
		host = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, fileName);
    out.writeLong(size);
    //out.writeLong(index);
    out.writeInt(index);
		out.writeLong(offset); // Is it needed?
		Text.writeString(out, host);
  }
}
