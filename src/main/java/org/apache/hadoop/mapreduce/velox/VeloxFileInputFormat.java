/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.velox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InvalidInputException;

/** 
 * A base class for file-based {@link InputFormat}s.
 * 
 * <p><code>VeloxFileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobContext)}.
 * Subclasses of <code>VeloxFileInputFormat</code> can also override the 
 * {@link #isSplitable(JobContext, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VeloxFileInputFormat extends TextInputFormat{
  ///**
  // * A factory that makes the split for this class. It can be overridden
  // * by sub-classes to make sub-types
  // */
  //protected FileSplit makeSplit(Path file, long start, long length, 
  //                              String[] hosts) {
  //  return new FileSplit(file, start, length, hosts);
  //}
  //
  ///**
  // * A factory that makes the split for this class. It can be overridden
  // * by sub-classes to make sub-types
  // */
  //protected FileSplit makeSplit(Path file, long start, long length, 
  //                              String[] hosts, String[] inMemoryHosts) {
  //  return new FileSplit(file, start, length, hosts, inMemoryHosts);
  //}

  /** 
   * Generate the list of files and make them into FileSplits.
   * @param job the job context
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    StopWatch sw = new StopWatch().start();
    //long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    //long maxSize = getMaxSplitSize(job);

    // generate splits
    //LOG.info("VeloxInputFormat called");
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(job, path)) {
        /*
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                        blkLocations[blkIndex].getHosts(),
                        blkLocations[blkIndex].getCachedHosts()));
            bytesRemaining -= splitSize;
          }
      
          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                       blkLocations[blkIndex].getHosts(),
                       blkLocations[blkIndex].getCachedHosts()));
          }
          */

          for(BlockLocation blkLocation : blkLocations) {
            splits.add(makeSplit(path, blkLocation.getOffset(), blkLocation.getLength(),
                        blkLocation.getHosts(),
                        blkLocation.getCachedHosts()));
          }
        } else { // not splitable
          splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(),
                      blkLocations[0].getCachedHosts()));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    sw.stop();
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
//          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
//    }
//      LOG.info("Total # of splits generated by getSplits: " + splits.size()
//          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    return splits;
  }

}
