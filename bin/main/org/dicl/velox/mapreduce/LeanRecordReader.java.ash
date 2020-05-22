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
//import org.apache.zookeeper.Stat;
import org.apache.zookeeper.data.Stat;
//zzunny
import java.net.*;
import java.util.*;


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


	// zzunny
	public InetAddress local;
	public String localhost;
	//public HashMap<String, List<Chunk>> map = new HashMap();
	// public List<Integer> change_idx = new ArrayList<Integer>();
	public int [] change_idx = new int[3];
	public int endFlag = 0;

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
    String zkAddress   = conf.get("velox.recordreader.zk-addr", "172.20.1.80:2381");
    zk = new ZooKeeper(zkAddress, DEFAULT_ZOOKEEPER_TIMEOUT_MS, (Watcher)isConnected);
		

    zkPrefix = "/chunks/" + context.getJobID() + "/"; 
    // zk.getChildren(zkPrefix, true, this, null);

		for(Chunk chunk : this.split.chunks) {
			try {
				if(zk.exists(zkPrefix + chunk.host, false) == null) {
					zk.create(zkPrefix + chunk.host, (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch(Exception e) {
				LOG.error("zookeeper error in initialize recordreader");
			}
		
		}

		String tmp_host = this.split.chunks.get(0).host;
		int ArrayIdx = 0;
		int tmp = 0;
	
		for(Chunk chunk : this.split.chunks){
			String cur_host = chunk.host;
			LOG.info("tmp = " + String.valueOf(tmp) + "ArrayIdx: " + String.valueOf(ArrayIdx) + "chunk.fileName = " + chunk.fileName + "chunk.index = " + chunk.index + " chunk.host = " + chunk.host);
			tmp++;
			//LOG.info("tmp host  = " + tmp_host + " cur host = " + cur_host);
			if(!cur_host.equals(tmp_host)) {
				ArrayIdx++;
				tmp_host = chunk.host;
				change_idx[ArrayIdx]++;
				continue;
			}
			tmp_host = chunk.host;
			change_idx[ArrayIdx]++;
		}
		change_idx[1] += change_idx[0];
		change_idx[2] += change_idx[1];
		LOG.info("change_idx[0] = " + String.valueOf(change_idx[0]));
		LOG.info("change_idx[1] = " + String.valueOf(change_idx[1]));
		LOG.info("change_idx[2] = " + String.valueOf(change_idx[2]));

		try {
    	local = InetAddress.getLocalHost();
    	localhost = local.getHostAddress();
    	LOG.info("local ip = " + localhost);
		} catch (UnknownHostException e) {
    	e.printStackTrace();
		}
		
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

    long start = 0;
    long end = 0;
		LOG.info("currentSplitNumChunks = " + String.valueOf(currentSplitNumChunks));	
		int replicaId = 0;

		while(processedChunks < currentSplitNumChunks) {
			LOG.info("replicaId = " + String.valueOf(replicaId));
			
			if(currentchunk >= change_idx[replicaId])	{
				LOG.info("CURRENT ENDFLAG = " + String.valueOf(endFlag));
				replicaId++;
				if(endFlag == 0) endFlag++;
				if(replicaId == 3) return -1;
				currentchunk = change_idx[replicaId] - 1;
			}
			
			LOG.info("replicaId = " + String.valueOf(replicaId));
			LOG.info("currentchunk = " + String.valueOf(currentchunk) + " change_idx[replicaId] = " + String.valueOf(change_idx[replicaId]));
			//if(currentchunk < change_idx[replicaId]) {
			if(currentchunk < change_idx[replicaId] && endFlag == 0) {

							Chunk chunk = split.chunks.get(currentchunk);
							LOG.info("now processing chunk.host = " + chunk.host + " chunk.index = " + chunk.index + " chunk.name = " + chunk.fileName);
							String chunkPath = zkPrefix + chunk.host + "/" + String.valueOf(chunk.index);
							start = System.currentTimeMillis();
							try {
											zk.create(chunkPath, (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
							} catch(KeeperException e) {
								LOG.info("someone stole my own chunk");
								LOG.info("STOP HERE ====== CHUNK.HOST = " + chunk.host + " currentchunk = " + currentchunk);
								end = System.currentTimeMillis();
								zookeeperTime += (end - start);

								currentchunk = change_idx[replicaId];
								processedChunks += (change_idx[replicaId] -1) - currentchunk; 
								continue;
								//return -1;

								//currentchunk++;
								//continue;
							} catch(Exception e) {
								LOG.error("Fails to connect to zookeeper");
								
							}
							end = System.currentTimeMillis();
								zookeeperTime += (end - start);
							LOG.info("NEW CHUNK FROM ME: " + currentchunk + "\trealindex: " + chunk.index + " offset : " + chunk.offset + " host: " + chunk.host);
							localChunks.add(chunk);
							processedChunks++;
							currentchunk++;
							size += chunk.size;
							/*
							if(currentchunk >= change_idx[replicaId]){
								chunkPath = zkPrefix + chunk.host + "/End";
								try {
									zk.create(chunkPath, (new String("End").getBytes()), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
								} catch(KeeperException e){
									return -1;
								} catch(Exception e){
									LOG.error("Fails to connect to zookeeper");
								}
							}*/
			}
			else {
							
							//if(replicaId == 3) return -1;
							//replicaId++;
							//currentchunk = change_idx[replicaId] - 1;
						
							Chunk chunk = split.chunks.get(currentchunk);
							LOG.info("try stealing now processing chunk.host = " + chunk.host + " chunk.index = " + chunk.index + " chunk.name = " + chunk.fileName);
							String chunkPath = zkPrefix + chunk.host + "/" + String.valueOf(chunk.index);
							start = System.currentTimeMillis();
							try {
									//Stat end_stat = zk.exists(zkPrefix+chunk.host+"/End", false);
									//if(end_stat == null){
										zk.create(chunkPath, (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
									//}
										/*
									else{
										LOG.info("===============the friend already done, try another friend========");
										endFlag++;
										currentchunk = change_idx[replicaId];
										end = System.currentTimeMillis();
										zookeeperTime += (end - start);
										if(endFlag == 3) {
											LOG.info("END HELPING FRIEND NOTHING TO DO ANYMORE");
											return -1;
										}	
										continue;
									}*/
							} catch(KeeperException e) {
								LOG.info("while helping keeper exception hadppend");
								if(currentchunk == change_idx[replicaId - 1]) {
							
								endFlag++;
								currentchunk = change_idx[replicaId];
								end = System.currentTimeMillis();
								zookeeperTime += (end - start);
								if(endFlag == 3) {
									return -1;
								}
								}
								currentchunk--;
								processedChunks++;
								continue;
							} catch(Exception e) {
								LOG.error("Fails to connect to zookeeper");
							}
							LOG.info("NEW CHUNK FROM STEAL: " + currentchunk + "\trealindex: " + chunk.index + " offset : " + chunk.offset + " host: " + chunk.host);
							localChunks.add(chunk);
							processedChunks++;
							currentchunk--;
							size += chunk.size;

			}
			break;
		}
				
		// reached EOF
		if (processedChunks == currentSplitNumChunks) {
			//LOG.info("I've done actually, but I want more....");
			LOG.info("NOTHING TO DO ANY MORE");
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

		//LOG.info("POS : " + pos );
    // Get new chunk if no bytes to read
    if (pos >= size) {
      getNextChunk();
    }

    // Find chunk to read
    final long startTime = System.currentTimeMillis();

    for (Chunk chunk : localChunks) {
			//LOG.info("LocalChunk " + chunk.fileName + " off : " + chunk.offset + " i : " + i );
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
	//	long chunkOffset = theChunk.offset 
    final int lenToRead = (int)Math.min(len, theChunk.size - chunkOffset);
		//LOG.info(lenToRead + " = " +len + " VS " + theChunk.size + " - " + chunkOffset);
		//LOG.info("theChunk.fileName : " + theChunk.fileName + " off: " + theChunk.offset + " host:" + split.host);
    final long readBytes = vdfs.readChunk(theChunk.fileName, split.host, buf, off, 
        theChunk.offset, lenToRead);

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

    //LOG.info(split.logicalBlockName + " " + String.valueOf(processedChunks));
    readingCounter.increment(readingTime);
    overheadCounter.increment(zookeeperTime);
    startOverheadCounter.increment(endConnect - startConnect);
    nextOverheadCounter.increment(nextTime);
  }
}
