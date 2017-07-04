// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*- 

/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * 
 * Implements the Hadoop FS interfaces to allow applications to store
 * files in Velox.
 */
package org.apache.hadoop.fs.velox;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;

import velox.VeloxDFS;

/**
 * <p>
 * An {@link FSInputStream} for a VeloxFileSystem and corresponding
 * VeloxDFS instance.
 */
public class VeloxInputStream extends FSDataInputStream {
  private static final Log LOG = LogFactory.getLog(VeloxInputStream.class);
  private boolean closed;

  private long fd;
  private long mPos;

  private VeloxDFS vdfs;

  /**
   * Create a new VeloxInputStream.
   * @param conf The system configuration. Unused.
   * @param fh The file descriptor provided by Velox to reference.
   * @param flength The current length of the file. If the length changes
   * you will need to close and re-open it to access the new data.
   */
  public VeloxInputStream(VeloxDFS _vdfs, long _fd) {
    vdfs = _vdfs;
    fd = _fd;
    closed = false;
    mPos = 0;
  }

  /** Velox likes things to be closed before it shuts down,
   * so closing the IOStream stuff voluntarily in a finalizer is good
   */
  protected void finalize() throws Throwable {
    try {
      if (!closed) {
        close();
      }
    } finally {
      super.finalize();
    }
  }

  /**
   * Find the number of bytes remaining in the file.
   */
  @Override
  public synchronized int available() throws IOException {
    return !closed;
  }

  public synchronized void seek(long targetPos) throws IOException {
    LOG.trace("[" + VeloxInputStream.class.toString() + "] seek: " + String.valueOf(targetPos));
    mPos = targetPos;
  }

  /**
   * Failovers are handled by the Velox code at a very low level;
   * if there are issues that can be solved by changing sources
   * they'll be dealt with before anybody even tries to call this method!
   * @return false.
   */
  public synchronized boolean seekToNewSource(long targetPos) {
    return false;
  }
    
  /**
   * Read a byte from the file.
   * @return the next byte.
   */
  @Override
  public synchronized int read() throws IOException {
    LOG.trace(
        "VeloxInputStream.read: Reading a single byte from fd " + fd
        + " by calling general read function");

    byte result[] = new byte[1];
    int readBytes = read(mPos, result, 0, 1);
    return (readBytes <= 0) ? -1 : (256 + (int)result[0]) % 256;
  }

  /**
   * Read a specified number of bytes from the file into a byte[].
   * @param pos position to seek
   * @param buf buffer to read
   * @param off offset to read in buffer
   * @param len length to read
   * @return 0 if successful, otherwise an error code.
   * @throws IOException on bad input.
   */
  @Override
  public synchronized int read(long pos, byte buf[], int off, int len)
    throws IOException {
    LOG.trace("VeloxInputStream.read: Reading " + len + " bytes from fd " + fd);

    long readBytes = vdfs.read(fd, pos, buf, off, len); 
    mPos += readBytes;

    return readBytes;
  }

  /**
   * Close the VeloxInputStream and release the associated filehandle.
   */
  @Override
  public void close() throws IOException {
    LOG.trace("VeloxOutputStream.close:enter");
    if (!closed) {
      vdfs.close(fd);

      closed = true;
      LOG.trace("VeloxOutputStream.close:exit");
    }
  }
}
