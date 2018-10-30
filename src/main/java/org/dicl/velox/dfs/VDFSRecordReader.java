package org.dicl.velox.dfs;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.dicl.velox.VeloxDFS;
import org.apache.hadoop.conf.Configuration;


import java.util.ArrayList;
import java.io.IOException;
import java.lang.InterruptedException;

public class VDFSRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(VDFSRecordReader.class);

    private VeloxDFS vdfs = null;
    private long pos = 0;
    private long size = 0;
    private int fd = 0;
    private int bufferOffset = 0;
    private int remaining_bytes = 0;
    private byte[] buffer;
    private byte[] lineBuffer;
    private static final int DEFAULT_BUFFER_SIZE = 2 << 20; // 2 MiB
    private static final int DEFAULT_LINE_BUFFER_SIZE = 8 << 10; // 8 KiB
    private LongWritable key;
    private Text value;
    private VDFSInputSplit split;

    public VDFSRecordReader() { 

    }
    /**
     * Called once at initialization.
     * @param split the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split_, TaskAttemptContext context) 
        throws IOException, InterruptedException {
        VDFSInputSplit split = (VDFSInputSplit) split_;

        vdfs = new VeloxDFS();
        Configuration conf = context.getConfiguration();

        int bufferSize =     conf.getInt("fs.velox.recordreader.buffersize", DEFAULT_BUFFER_SIZE);
        int lineBufferSize = conf.getInt("fs.velox.recordreader.linebuffersize", DEFAULT_LINE_BUFFER_SIZE);

        buffer = new byte[bufferSize];
        lineBuffer = new byte[lineBufferSize];
        LOG.info("Initialized RecordReader for: " + split.logical_block_name + " size: " + split.size);
    }

    /**
     * Read the next key, value pair.
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (pos >= size)
            return false;

        // Computing key
        key.set(pos);

        // Computing value
        String line = "";
        int lpos = 0;
        while (lpos < DEFAULT_LINE_BUFFER_SIZE) {
            byte c = read();
            if (c == '\n') {
                line = new String(lineBuffer, 0, lpos);
                break;
            } 
            //else if (c == -1) {
             //   return false;
            //}

            lineBuffer[lpos++] = c;
        }
        value.set(line);
        return true;
    }

    private byte read() {
        bufferOffset %= buffer.length;
        if (bufferOffset == 0 || remaining_bytes == 0) {
            bufferOffset = 0;
            remaining_bytes = read(pos, buffer, bufferOffset, buffer.length);
        }

        byte ret = buffer[bufferOffset];

        // Increment/decrement counters
        pos++;
        remaining_bytes--;
        bufferOffset++;

        return ret;
    }
    public int read(long pos, byte[] buf, int off, int len) {

        // Choose chunk
        int i = 0; long total_size = 0;
        ArrayList<Chunk> chunks = split.chunks;
        for (Chunk chunk : chunks) {
            if (chunk.size + total_size > pos) {
                return i;
            }
            total_size += chunk.size;
            i++;
        }

        long chunk_offset = pos - total_size;

        //Chunk the_chunk = chunks.at(i);

//        vdfs.read_chunk(the_chunk.name, the_chunk.host, chunk_offset, buf, off, len);

        return 1;
    }

    /**
     * Get the current key
     * @return the current key or null if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * Get the current value.
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * The current progress of the record reader through its data.
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float)pos/(float)size;
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException {
        vdfs.close(fd);
    }
}
