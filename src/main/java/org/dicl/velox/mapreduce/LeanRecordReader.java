package org.dicl.velox.mapreduce;

import com.dicl.velox.VeloxDFS;

import java.io.IOException;
import java.lang.Boolean;
import java.lang.InterruptedException;
import java.lang.Math;
import java.lang.System;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class LeanRecordReader extends RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(LeanRecordReader.class);

  // Constants
  private static final int DEFAULT_BUFFER_SIZE = 2 << 20; // 2 MiB
  private static final int DEFAULT_LINE_BUFFER_SIZE = 8 << 20; // 8 MiB
  private static final int DEFAULT_ZOOKEEPER_TIMEOUT_MS = 180000; // 180s

  // Hadoop stuff
  private LongWritable key = new LongWritable();
  private Text value = new Text();
  private LeanInputSplit split;
  private String zkPrefix;
  private Counter inputCounter;
  private Counter overheadCounter;
  private Counter readingCounter;
  private Counter startOverheadCounter;
  private Counter nextOverheadCounter;

  private VeloxDFS vdfs = null;
  private long pos = 0;
  private long size = 0;
  private long processedChunks = 0;
  private int bufferOffset = 0;
  private int remainingBytes = 0;
  private byte[] buffer;
  private int currentchunk = 0;
  private ArrayList<Chunk> localChunks = new ArrayList<Chunk>();
  private int numStaticChunks;
  private int currentSplitNumChunks;

  // Zookeeper stuff
  private ZooKeeper zk;
  private Future<Boolean> isConnected;

  // Profiling stuff
  private long zookeeperTime = 0;
  private long readingTime = 0;
  private long nextTime = 0;
  private long startConnect = 0;
  private long endConnect = 0;
  private boolean first = true;
  private byte[] lineBuffer;

  /**
   * Zookeeper watcher to manage when we are actually connected to zookeeper.
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
   * @throws IOException idk
   * @throws InterruptedException idk
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) 
      throws IOException, InterruptedException {
    startConnect = System.currentTimeMillis();
    this.split = (LeanInputSplit) split;
    size = 0;

    currentSplitNumChunks = this.split.chunks.size();

    vdfs = new VeloxDFS();
    Configuration conf = context.getConfiguration();

    int bufferSize     = conf.getInt("velox.recordreader.buffersize", DEFAULT_BUFFER_SIZE);
    int lineBufferSize = conf.getInt("velox.recordreader.linebuffersize", DEFAULT_LINE_BUFFER_SIZE);
    numStaticChunks    = conf.getInt("velox.numStaticChunks", 0);

    buffer = new byte[bufferSize];
    lineBuffer = new byte[DEFAULT_LINE_BUFFER_SIZE];

    isConnected = new ZKconnectCallable();
    String zkAddress   = conf.get("velox.recordreader.zk-addr", "192.168.0.101:2181");
    zk = new ZooKeeper(zkAddress, DEFAULT_ZOOKEEPER_TIMEOUT_MS, (Watcher)isConnected);

    zkPrefix = "/chunks/" + context.getJobID() + "/"; 
    // Shuffle chunks to avoid access contention
    //Collections.shuffle(split.chunks);

    inputCounter = context.getCounter("Lean COUNTERS", LeanInputFormat.Counter.BYTES_READ.name());
    overheadCounter = context.getCounter("Lean COUNTERS", "ZOOKEEPER_OVERHEAD_MILISECONDS");
    readingCounter = context.getCounter("Lean COUNTERS", "READING_OVERHEAD_MILISECONDS");
    startOverheadCounter = context.getCounter("Lean COUNTERS", "START_OVERHEAD_MILISECONDS");
    nextOverheadCounter = context.getCounter("Lean COUNTERS", "NEXT_OVERHEAD_MILISECONDS");

    LOG.info("Initialized RecordReader for: " + this.split.logicalBlockName
        + " size: " + size + " NumChunks: " + this.split.chunks.size()
        + " Host " + this.split.host + " staticchunks " + numStaticChunks);
  }

  /**
   *  Try to allocate a chunk to be processed.
   *  @return ID of the allocated chunk; -1 when no chunks are available anymore
   */
  private int getNextChunk() {

    try {
      if (!isConnected.get()) {
        LOG.error("RecordReader failed to connect to the ZK instance");
      }
    } catch (Exception e) {
      LOG.error("Messed up with concurrency");
    }

    if (first) {
      endConnect = System.currentTimeMillis();
      first = false;
    }

    // Try to create a node Atomic operation
    while (currentchunk < currentSplitNumChunks) {

      Chunk chunk = split.chunks.get(currentchunk);

      // Dynamic chunk selection
      if (chunk.index >= numStaticChunks -1) {

        String chunkPath = zkPrefix + String.valueOf(chunk.index);
        long start = 0;
        long end = 0;

        try {
          start = System.currentTimeMillis();
          zk.create(chunkPath, (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, 
              CreateMode.PERSISTENT); 

          // Already exists
        } catch (KeeperException e) {
          currentchunk++;
          continue;

        } catch (Exception e) {
          LOG.error("Fails to connect to zookeeper");

        } finally {
          end = System.currentTimeMillis();
          zookeeperTime += (end - start);
        }
      }

      // If we found a availible chunk
      LOG.info("I got a new chunk: " + currentchunk + " realindex: " + chunk.index);
      localChunks.add(chunk);
      processedChunks++;
      currentchunk++;
      size += chunk.size;
      break;
    }

    // Reached EOF
    if (currentchunk == currentSplitNumChunks) {
      return -1;
    }

    return currentchunk;
  }

  /**
   * Read the next key, value pair.
   * @return true if a key/value pair was read
   * @throws IOException idk
   * @throws InterruptedException idk
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean isEOF = false;
    // Computing key
    final long startTime = System.currentTimeMillis();
    key.set(pos);

    // Computing value
    String line = "";
    int lpos = 0;

    lineBuffer[0] = 0;
    while (lpos < DEFAULT_LINE_BUFFER_SIZE) {
      byte c = read();
      if (c == '\n' || c == -1) {
        lineBuffer[lpos + 1] = 0;
        line = new String(lineBuffer, 0, lpos);
        if (c == -1) {
          isEOF = true;
        }
        break;
      }

      lineBuffer[lpos++] = c;
    }

    value.set(line);
    final long endTime = System.currentTimeMillis();
    nextTime += (endTime - startTime);
    return lpos > 0 || !isEOF;
  }

  /**
   * Read one character at the time.
   * @return the read character or -1 when EOF or ERROR
   */
  private byte read() {
    bufferOffset %= buffer.length;
    if (bufferOffset == 0 || remainingBytes == 0) {
      bufferOffset = 0;
      remainingBytes = read(pos, buffer, bufferOffset, buffer.length);
    }

    if (remainingBytes <= 0) {
      return -1;
    }

    final byte ret = buffer[bufferOffset];

    // Increment/decrement counters
    pos++;
    remainingBytes--;
    bufferOffset++;

    return ret;
  }

  /**
   * Read the chunk at the buffer.
   * @param pos the position in the logical block to read.
   * @param buf the buffer where to write the read bytes.
   * @param off the offset in the buffer to start writing the read files.
   * @param len the number of files to read in the logical block.
   * @return number of read bytes
   */
  public int read(long pos, byte[] buf, int off, int len) {
    int i = 0; 
    long totalSize = 0;

    // Get new chunk if no bytes to read
    if (pos >= size) {
      getNextChunk();
    }

    // Find chunk to read
    final long startTime = System.currentTimeMillis();

    for (Chunk chunk : localChunks) {
      if (chunk.size + totalSize > pos) {
        break;
      }
      totalSize += chunk.size;
      i++;
    }

    if (i == localChunks.size()) {
      return -1;
    }

    Chunk theChunk = localChunks.get(i);
    long chunkOffset = pos - totalSize;

    final int lenToRead = (int)Math.min(len, theChunk.size - chunkOffset);

    final long readBytes = vdfs.readChunk(theChunk.fileName, split.host, buf, off, 
        chunkOffset, lenToRead);

    final long endTime = System.currentTimeMillis();
    readingTime += (endTime - startTime);

    return (int)readBytes;
  }

  /**
   * Get the current key.
   * @return the current key or null if there is no current key
   * @throws IOException idk
   * @throws InterruptedException idk
   */
  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  /**
   * Get the current value.
   * @return the object that was read
   * @throws IOException idk
   * @throws InterruptedException idk
   */
  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  /**
   * The current progress of the record reader through its data.
   * @return a number between 0.0 and 1.0 that is the fraction of the data read
   * @throws IOException idk
   * @throws InterruptedException idk
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float)currentchunk / (float)currentSplitNumChunks;
  }

  /**
   * Close the record reader.
   */
  @Override
  public void close() throws IOException {
    try {
      zk.close();
    } catch (Exception e) { 
      LOG.error("Fails to close the connection to ZooKeeper");
    }

    inputCounter.increment(pos);

    LOG.info(split.logicalBlockName + " " + String.valueOf(processedChunks));
    readingCounter.increment(readingTime);
    overheadCounter.increment(zookeeperTime);
    startOverheadCounter.increment(endConnect - startConnect);
    nextOverheadCounter.increment(nextTime);
  }
}
