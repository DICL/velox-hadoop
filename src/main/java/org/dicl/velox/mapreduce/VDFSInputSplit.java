package org.dicl.velox.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.Object;

public class VDFSInputSplit extends InputSplit implements Writable {
    public ArrayList<Chunk> chunks;
    public String logical_block_name;
    public String jobID;
    public String host;
    public long size;
	public long taskID;

    public VDFSInputSplit() {
        chunks = new ArrayList<Chunk>();
    }

    public VDFSInputSplit(String name, String jobID, int taskID) {
        chunks = new ArrayList<Chunk>();
        this.logical_block_name = name;
        this.jobID = jobID;
		this.taskID = taskID;
    }

    //public void addChunk(String fname, long size, long seq, long offset, String host) {
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
        logical_block_name = Text.readString(in);
        jobID = Text.readString(in);
        host = Text.readString(in);
        taskID = in.readLong();
        ArrayWritable in_chunks = new ArrayWritable(Chunk.class);
        in_chunks.readFields(in);
        chunks = new ArrayList<Chunk>(Arrays.asList((Chunk[])in_chunks.toArray()));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, logical_block_name);
        Text.writeString(out, jobID);
        Text.writeString(out, host);
        out.writeLong(taskID);
        ArrayWritable in_chunks = new ArrayWritable(Chunk.class,
                chunks.toArray(new Chunk[chunks.size()]));
        in_chunks.write(out);
    }
}
