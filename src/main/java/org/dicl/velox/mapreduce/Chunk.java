package org.dicl.velox.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Chunk implements Writable {
    public String file_name;
    public long size;
    public long index;

    public Chunk()  { }

    public Chunk(String file_name, long size, long index)  {
        this.file_name = file_name;
        this.index = index;
        this.size = size;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file_name = Text.readString(in);
        size = in.readLong();
        index = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file_name);
        out.writeLong(size);
        out.writeLong(index);
    }
}
