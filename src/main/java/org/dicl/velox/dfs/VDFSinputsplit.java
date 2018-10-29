package org.dicl.velox.dfs;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import java.util.ArrayList;

public class VDFSInputSplit extends InputSplit implements Writable {
    private ArrayList chunks;
    private String logical_block_name;
    private host;

    public VDFSInputSplit(String name, string host) {
        this.logical_block_name = name;
        this.host = host;
    }

    public addChunk(String fname, long size, long seq) {
        chunks.add(new Chunk(fname, size, seq));
    }

    @Override
    public String[] getLocations() throws IOException {
        return host;
    }

    @Override
    public long getLength() throws IOException {
        return chunks.stream().mapToInt(o -> o.getSize()).sum(); // YAY Functional
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        logical_block_name = Text.readString(in);
        ArrayWritable in_chunks = new ArrayWritable(Chunk.class);
        in_chunks.readFields(in)
        chunks = Arrays.asList(in_chunks.toArray());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file_name);
        ArrayWritable in_chunks = new ArrayWritable(Chunk.class,
                chunks.toArray(new Chunk[chunks.size()]));
        in_chunks.write(out);
    }

}
