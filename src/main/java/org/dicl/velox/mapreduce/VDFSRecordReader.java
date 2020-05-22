package org.dicl.velox.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.dicl.velox.VeloxDFS;

import java.util.ArrayList;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.Math;

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
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private VDFSInputSplit split;
    private Counter inputCounter;
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
        split = (VDFSInputSplit) split_;
        size = split.size;

        vdfs = new VeloxDFS(split.jobID, split.taskID, false);
        Configuration conf = context.getConfiguration();

        int bufferSize =     conf.getInt("velox.recordreader.buffersize", DEFAULT_BUFFER_SIZE);
        int lineBufferSize = conf.getInt("velox.recordreader.linebuffersize", DEFAULT_LINE_BUFFER_SIZE);

        buffer = new byte[bufferSize];
        lineBuffer = new byte[lineBufferSize];

        inputCounter = context.getCounter("VDFS COUNTERS", VDFSInputFormat.Counter.BYTES_READ.name());
        LOG.info("Initialized RecordReader for: " + split.logical_block_name);
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
            } else if (c == -1) {
                return false;
            }

            lineBuffer[lpos++] = c;
        }
        value.set(line);
        return true;
    }

    /**
     * Read one character at the time
     * @return the read character or -1 when EOF or ERROR
     */
    private byte read() {
        bufferOffset %= buffer.length;
        if (bufferOffset == 0 || remaining_bytes == 0) {
            bufferOffset = 0;
            remaining_bytes = read(pos, buffer, bufferOffset, buffer.length);
        }

        if (remaining_bytes <= 0) {
            return -1;
        }

        byte ret = buffer[bufferOffset];

        // Increment/decrement counters
        pos++;
        remaining_bytes--;
        bufferOffset++;

        return ret;
    }

    /**
     * Read the chunk at the buffer
     * @param pos the position in the logical block to read.
     * @param buf the buffer where to write the read bytes.
     * @param off the offset in the buffer to start writing the read files.
     * @param len the number of files to read in the logical block.
     * @return number of read bytes
     */
    public int read(long pos, byte[] buf, int off, int len) {
        int i = 0; long total_size = 0;

        long readBytes = vdfs.readChunk(buf, off);
		if(readBytes == 0)
			return -1;

        return (int)readBytes;
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
        inputCounter.increment(pos);
    }
}
