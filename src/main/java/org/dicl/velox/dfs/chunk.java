package org.dicl.velox.dfs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Chunk implements Writable {
    public String file_name;
    public long size;
    public long index;

    public Chunk(String file_name, long size, long index)  {
        this.file_name = file_name;
        this.index = index;
        this.size = size;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file_name = Text.readString(in);
        size = in.readLong();
        seq = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file_name);
        out.writeLong(size);
        out.writeLong(index);
    }
}
