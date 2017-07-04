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
 * files in VeloxDFS.
 */

package org.apache.hadoop.fs.velox;

import velox.VDFS;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * An {@link OutputStream} for a VeloxFileSystem and corresponding
 * Velox instance.
 */
public class VeloxOutputStream extends FSDataOutputStream {
  private static final Log LOG = LogFactory.getLog(VeloxOutputStream.class);
  private boolean closed;

  private VDFS vdfs;

  private long fd;

  private byte[] buffer;
  private int bufUsed = 0;
  private int mPos = 0;

  /**
   * Construct the VeloxOutputStream.
   * @param fd File descriptor
   * @param bufferSize ??
   */
  public VeloxOutputStream(VDFS _vdfs, long _fd, int bufferSize) {
    vdfs = _vdfs;
    fd = _fd;
    closed = false;
    buffer = new byte[bufferSize];
  }

  /**
   * Close the Velox file handle if close() wasn't explicitly called.
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
   * Ensure that the stream is opened.
   */
  private synchronized void checkOpen() throws IOException {
    if (closed)
      throw new IOException("operation on closed stream (fd=" + fd + ")");
  }

  /**
   * Get the current position in the file.
   * @return The file offset in bytes.
   */
  public synchronized long getPos() throws IOException {
    //checkOpen();
    return mPos;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    byte buf[] = new byte[1];
    buf[0] = (byte) b;    
    write(buf, 0, 1);
  }

  @Override
  public synchronized void write(byte buf[], int off, int len) throws IOException {
    //checkOpen();

    while (len > 0) {
      int remaining = Math.min(len, buffer.length - bufUsed);
      System.arraycopy(buf, off, buffer, bufUsed, remaining);

      bufUsed += remaining;
      off += remaining;
      len -= remaining;

      if (buffer.length == bufUsed)
        flushBuffer();
    }
  }

  /*
   * Moves data from the buffer into vdfs.
   */
  private synchronized void flushBuffer() throws IOException {
    if (bufUsed == 0)
      return;
      
    long ret = vdfs.write(fd, mPos, buffer, 0, bufUsed);
    if (ret < 0)
      throw new IOException("vdfs.write: ret=" + ret);

    mPos += bufUsed;
    bufUsed = 0;
  }
   
  @Override
  public synchronized void flush() throws IOException {
    flushBuffer(); // buffer -> vdfs
  }
  
  @Override
  public synchronized void close() throws IOException {
    //checkOpen();
    flush();
    vdfs.close(fileHandle);
    closed = true;
    mPos = 0;
    bufUsed = 0;
  }
}
