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

// Zookeeper staff
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.Math;
import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.Boolean;
import java.lang.System;
import java.util.concurrent.ExecutionException;
import java.util.Collections;


public class LeanRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(LeanRecordReader.class);

    // Hadoop stuff
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private LeanInputSplit split;
    private Counter inputCounter;
    private Counter overheadCounter;
    private Counter readingCounter;
    private Counter chunksCounter;

    private VeloxDFS vdfs = null;
    private long pos = 0;
    private long size = 0;
    private long processedChunks= 0;
    private int fd = 0;
    private int bufferOffset = 0;
    private int remaining_bytes = 0;
    private byte[] buffer;
    private byte[] lineBuffer;
    private static final int DEFAULT_BUFFER_SIZE = 2 << 20; // 2 MiB
    private static final int DEFAULT_LINE_BUFFER_SIZE = 8 << 10; // 8 KiB
    private int currentchunk = 0;
    private ArrayList<Chunk> localChunks = new ArrayList<Chunk>();
    private double input_thre;
    private int numChunks;
    private int nChunks;

    // Zookeeper stuff
    private ZooKeeper zk;
    private Future<Boolean> isConnected;

    // Profiling stuff
    private boolean profileToHDFS = false;
    private long startTime = 0; 
    private long zookeeper_time = 0;
    private long reading_time = 0;
    private long startConnect= 0, endConnect;
    private FileSystem fileSystem;
    private Path path;
    private boolean first = true;


    /**
     * Zookeeper watcher to manage when we are actually connected to zookeeper
     */
    static class ZKconnectCallable extends CompletableFuture<Boolean> implements Watcher {
        @Override
        public void process(WatchedEvent event)  { 
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                complete(new Boolean(true)); 
            }
        }
    }

    public LeanRecordReader() { }

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
        startTime = System.currentTimeMillis();
        startConnect = System.currentTimeMillis();
        split = (LeanInputSplit) split_;
        size = 0;

        nChunks = split.chunks.size();

        vdfs = new VeloxDFS();
        Configuration conf = context.getConfiguration();

        int bufferSize     = conf.getInt("velox.recordreader.buffersize", DEFAULT_BUFFER_SIZE);
        int lineBufferSize = conf.getInt("velox.recordreader.linebuffersize", DEFAULT_LINE_BUFFER_SIZE);
        profileToHDFS      = conf.getBoolean("velox.profileToHDFS", false);
        String zkAddress   = conf.get("velox.recordreader.zk-addr", "192.168.0.101:2181");
        input_thre         = conf.getDouble("velox.input_threshold", 0.80);
        numChunks          = conf.getInt("velox.numChunks", 0);

        buffer = new byte[bufferSize];
        lineBuffer = new byte[lineBufferSize];

        ZKconnectCallable watcher = new ZKconnectCallable();
        isConnected = watcher;

        zk = new ZooKeeper(zkAddress, 180000, watcher);

        if (profileToHDFS) {
            path = new Path("hdfs:///stats_" + context.getJobID());
            fileSystem = FileSystem.get(conf);
        }

        // Shuffle chunks to avoid access contention
        //Collections.shuffle(split.chunks);

        inputCounter = context.getCounter("Lean COUNTERS", LeanInputFormat.Counter.BYTES_READ.name());
        overheadCounter = context.getCounter("Lean COUNTERS", "ZOOKEEPER_OVERHEAD_MILISECONDS");
        readingCounter = context.getCounter("Lean COUNTERS", "READING_OVERHEAD_MILISECONDS");
        LOG.info("Initialized RecordReader for: " + split.logical_block_name + " size: " + size + " NumChunks: " + split.chunks.size() + " Host " + split.host + " TotalChunks " + numChunks + " input_threshold" + input_thre);
    }

    /**
     *  Try to allocate a chunk to be processed
     *  @return ID of the allocated chunk; -1 when no chunks are available anymore
     */
    private int getNextChunk() {
        if (first) {
            endConnect = System.currentTimeMillis();
            first = false;
        }

        try {
            if (!isConnected.get())
                LOG.error("RecordReader failed to connect to the ZK instance");

            // Try to create a node Atomic operation
            while (currentchunk < nChunks) {
                Chunk chunk = split.chunks.get(currentchunk);

                if (chunk.index >= (int)(numChunks*input_thre)) {
                    String chunkPath = "/chunks/" + chunk.index;
                    long start = 0, end = 0;

                    try {
                        start = System.currentTimeMillis();
                        zk.create(chunkPath, (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 

                        // Already exists
                    } catch (KeeperException e) {
                        end = System.currentTimeMillis();
                        overheadCounter.increment(end - start);

                        currentchunk++;
                        continue;
                    }
                    end = System.currentTimeMillis();
                    zookeeper_time += (end - start);
                }

                LOG.info("I got a new chunk: "+ currentchunk + " realindex: " + chunk.index);
                localChunks.add(chunk);
                processedChunks++;
                currentchunk++;
                size += chunk.size;
                break;
            }
        } catch (Exception e) {
            LOG.error("Messed up with concurrency");
        }

        // Reached EOF
        if (currentchunk == nChunks) {
            return -1;
        }

        return currentchunk;
    }

    /**
     * Read the next key, value pair.
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
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

        // Get new chunk if no bytes to read
        if (pos >= size) {
            getNextChunk();
        }

        // Find chunk to read
        long start_time = System.currentTimeMillis();
        for (Chunk chunk : localChunks) {
            if (chunk.size + total_size > pos) {
               break;
            }
            total_size += chunk.size;
            i++;
        }

        if (i == localChunks.size()) {
            return -1;
        }

        Chunk the_chunk = localChunks.get(i);
        long chunk_offset = pos - total_size;

        int len_to_read = (int)Math.min(len, the_chunk.size - chunk_offset);

        long readBytes = vdfs.readChunk(the_chunk.file_name, split.host, buf, off, chunk_offset, len_to_read);
        long end_time = System.currentTimeMillis();
        reading_time += (end_time - start_time);

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
        return (float)currentchunk/(float)nChunks;
    }


    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException {
        long task_duration = System.currentTimeMillis() - startTime;
        
        try {
            zk.close();
        } catch (Exception e) { }

            while (profileToHDFS) {
                try{
                    FSDataOutputStream logFile = fileSystem.append(path);
                    logFile.writeChars(split.host + " " + split.logical_block_name + " "+  split.chunks.size() 
                            +" " + task_duration +" " + String.valueOf(processedChunks) +  " " + (endConnect - startConnect) + " " + zookeeper_time +" " + reading_time + " " 
                            + currentchunk + "\n");
                    logFile.hflush();
                    logFile.close();
                } catch (Exception e) {
                    continue;
                }
                break;
            }
        vdfs.close(fd);
        inputCounter.increment(pos);

        LOG.info(split.logical_block_name + " " + String.valueOf(processedChunks));
        readingCounter.increment(reading_time);
        overheadCounter.increment(zookeeper_time);
    }
}
