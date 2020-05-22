package org.dicl.velox.mapreduce;

import com.dicl.velox.VeloxDFS;

import java.io.IOException;
import java.lang.Boolean;
import java.lang.InterruptedException;
import java.lang.Math;
import java.lang.System;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

//zzunny
import java.net.*;
import org.apache.hadoop.util.LineReader;


public class LeanRecordReader_fixing extends RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(LeanRecordReader_fixing.class);

	// Constants
	private static final int DEFAULT_BUFFER_SIZE = 2 << 20; // 2 MiB
	private static final int DEFAULT_LINE_BUFFER_SIZE = 8 << 20; // 8 MiB

	// Hadoop stuff
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private LeanInputSplit split;
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

	// Profiling stuff
	private long readingTime = 0;
	private long nextTime = 0;
	private long startConnect = 0;
	private long endConnect = 0;
	private boolean first = true;
	private byte[] lineBuffer;
	private int processed = 0;
	private InputStream is = null;
	private BufferedReader bfReader = null;


	// zzunny
	private LineReader in;

	public LeanRecordReader_fixing() throws IOException {
	}

	public void initialize(InputSplit split, TaskAttemptContext context) 
		throws IOException, InterruptedException {
			startConnect = System.currentTimeMillis();
			this.split = (LeanInputSplit) split;
			size = 0;


			vdfs = new VeloxDFS(this.split.jobID, this.split.taskID, false);

			Configuration conf = context.getConfiguration();

			int bufferSize     = conf.getInt("velox.recordreader.buffersize", DEFAULT_BUFFER_SIZE);
			int lineBufferSize = conf.getInt("velox.recordreader.linebuffersize", DEFAULT_LINE_BUFFER_SIZE);

			buffer = new byte[bufferSize];
			lineBuffer = new byte[DEFAULT_LINE_BUFFER_SIZE];

			inputCounter = context.getCounter("Lean COUNTERS", LeanInputFormat.Counter.BYTES_READ.name());
			readingCounter = context.getCounter("Lean COUNTERS", "READING_OVERHEAD_MILISECONDS");
			startOverheadCounter = context.getCounter("Lean COUNTERS", "START_OVERHEAD_MILISECONDS");
			nextOverheadCounter = context.getCounter("Lean COUNTERS", "NEXT_OVERHEAD_MILISECONDS");
			endConnect = System.currentTimeMillis();


			LOG.info("Initialized RecordReader for: " + this.split.logicalBlockName	+ " size: " + size 	+ " Host " + this.split.host );
		}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean isEOF = false;
		// Computing key
		final long startTime = System.currentTimeMillis();
		key.set(pos);

		// Computing value
		String line = "";

		int lpos = 0;
		int newSize = 0;

		lineBuffer[0] = 0;
		while (lpos < DEFAULT_LINE_BUFFER_SIZE) {
			if(remainingBytes == 0) {
				remainingBytes = read(pos, buffer, bufferOffset, buffer.length);
				is = new ByteArrayInputStream(buffer);
				bfReader = new BufferedReader(new InputStreamReader(is));
			}
			if(remainingBytes <= 0) {
				return false;
			}

			try {
				if((line = bfReader.readLine()) != null) {
					LOG.info(line);
					remainingBytes -= line.length();
					break;
				} else {
					remainingBytes = 0;

				}
			} catch(Exception e) {

			}

			/*
			bufferOffset %= buffer.length;
			if (bufferOffset == 0 || remainingBytes == 0) {
				LOG.info("Read chunk from shared memory");
				bufferOffset = 0;
				remainingBytes = read(pos, buffer, bufferOffset, buffer.length);
			}
			if (remainingBytes <= 0) {
				return false;
			}
			try {
				is = new ByteArrayInputStream(buffer,0,lpos);
				bfReader = new BufferedReader(new InputStreamReader(is));
				
				if((line = bfReader.readLine()) != null) {
					LOG.info(line);
					pos += line.length();
					remainingBytes -= line.length();
					bufferOffset += line.length();
					break;
				} else {
					isEOF = true;
					return false;
				}
			} catch(Exception e) {

			}
			//byte c = read();
			
			if (c == '\n' || c == -1) {
				lineBuffer[lpos + 1] = 0;
				line = new String(lineBuffer, 0, lpos);
				LOG.info("line: " + line);
				if (c == -1) {
					isEOF = true;
				}
				break;
			}
			
			else {
				LOG.info("not new line character");
			}

			lineBuffer[lpos++] = c;
			*/
		}
		value.set(line);
		//value.set(lineBuffer);
		//LOG.info("key = " + key.toString() + " value = " + line);
		final long endTime = System.currentTimeMillis();
		nextTime += (endTime - startTime);
		return lpos > 0 || !isEOF;
	}

	private byte read() {
		bufferOffset %= buffer.length;
		if (bufferOffset == 0 || remainingBytes == 0) {
			LOG.info("Read chunk from shared memory");
			bufferOffset = 0;
			remainingBytes = read(pos, buffer, bufferOffset, buffer.length);
		}

		if (remainingBytes <= 0) {
			return -1;
		}

		final byte ret = buffer[bufferOffset];
		//String str = String.valueOf((char)ret);
		//LOG.info(str);

		// Increment/decrement counters
		pos++;
		remainingBytes--;
		bufferOffset++;

		return ret;
	}

	public int read(long pos, byte[] buf, int off, int len) {
		int i = 0; 
		long totalSize = 0;
		long readBytes = 0;

		// Get new chunk if no bytes to read
		if (pos >= size) {
			readBytes = vdfs.readChunk(buf, off);
			//LOG.info("[vdfs.readChunk] readBytes: " + Long.toString(readBytes) + " off: " + Integer.toString(off));
			processed++;
		}

		if (readBytes <= 0) {
			return -1;
		}

		return (int)readBytes;
	}

	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	public float getProgress() throws IOException, InterruptedException {
		return (float)processed / 40;
	}

	public void close() throws IOException {
		try {
			inputCounter.increment(pos);
			readingCounter.increment(readingTime);
			startOverheadCounter.increment(endConnect - startConnect);
			nextOverheadCounter.increment(nextTime);
		} catch (Exception e) { 
			LOG.error("Fails to close the connection to ZooKeeper");
		}

		
	}
}
