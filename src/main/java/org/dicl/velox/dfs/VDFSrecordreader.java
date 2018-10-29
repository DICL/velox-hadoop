package org.dicl.velox.dfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.dicl.velox.VeloxDFS;

public class VDFSRecordReader extends RecordReader<LongWritable, Text> implements Writable {
    private static final Log LOG = LogFactory.getLog(VDFSRecordReader.class);

    private VeloxDFS vdfs = null;
    private long pos = 0;
    private long size = 0;
    private int fd = 0;
    private long bufferOffset = 0;
    private long remaining_bytes = 0;
    private byte[] buffer;
    private byte[] lineBuffer;
    private static final int DEFAULT_BUFFER_SIZE = 2 << 20; // 2 MiB
    private static final int DEFAULT_LINE_BUFFER_SIZE = 8 << 10; // 8 KiB
    private LongWritable key;
    private Text value;

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
    public void initialize(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException {
        VDFSInputSplit split = (VDFSInputSplit) split;

        vdfs = new VeloxDFS();
        fd = vdfs.open(split.file_name);
        Configuration conf = context.getConfiguration();

        long bufferSize =     conf.getInt("fs.velox.recordreader.buffersize", DEFAULT_BUFFER_SIZE);
        long lineBufferSize = conf.getInt("fs.velox.recordreader.linebuffersize", DEFAULT_LINE_BUFFER_SIZE);

        buffer = new byte[bufferSize];
        lineBuffer = new byte[lineBufferSize];
        LOG.info("Initialized RecordReader for: " + split.file_name + " size: " + split.size);
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
        String line;
        long lpos = 0;
        while (lpos < DEFAULT_LINE_BUFFER_SIZE) {
            char c = read();
            if (c == '\n') {
                line = new String(lineBuffer, 0, lpos);
                break;
            } else if ((int)c == -1) {
                return false
            }

            lineBuffer[lpos++] = c;
        }
        value.set(line);
    }

    private char read() {
        bufferOffset %= buffer.length;
        if (bufferOffset == 0 || remaining_bytes == 0) {
            bufferOffset = 0;
            remaining_bytes = read_logical(pos, buffer, bufferOffset, buffer.length);
        }

        char ret = buffer[bufferOffset];

        // Increment/decrement counters
        pos++;
        remaining_bytes--;
        bufferOffset++;

        return ret;
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
