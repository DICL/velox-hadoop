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
package org.dicl.velox.fs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;

import com.dicl.velox.VeloxDFS;

/**
 * <p>
 * An {@link FSInputStream} for a VeloxFileSystem and corresponding
 * VeloxDFS instance.
 */
public class VeloxFSInputStream extends FSInputStream {
  private static final Log LOG = LogFactory.getLog(VeloxFSInputStream.class);

  private boolean closed = false;
  private long fd = 0;
  private long mPos = 0;
  private long fileSize = 0;
  private long remaining_bytes= 0;
  private long total_read_bytes= 0;
  private byte[] buffer;
  private int bufferOffset = 0;
  private boolean EOF = false;

  private VeloxDFS vdfs = null;

  private static final int DEFAULT_BUFFER_SIZE = 1 << 21; // 2 MiB

  /**
   * Create a new VeloxFSInputStream.
   * @param conf The system configuration. Unused.
   * @param fh The file descriptor provided by Velox to reference.
   * @param flength The current length of the file. If the length changes
   * you will need to close and re-open it to access the new data.
   */
  public VeloxFSInputStream() {
    super();

    buffer = new byte[DEFAULT_BUFFER_SIZE];
  }

  public VeloxFSInputStream(VeloxDFS vdfs, long fd, int bufferSize, long fileSize) {
    this.vdfs = vdfs;
    this.fd = fd;

    this.fileSize = fileSize;
    bufferSize = (int)Math.min((long)bufferSize, fileSize);
    bufferSize = (int)Math.min((long)bufferSize, DEFAULT_BUFFER_SIZE);
    this.buffer = new byte[bufferSize];
    LOG.info("Constructor finished for VeloxFSInputStream b:"+bufferSize + " f:" + fileSize);
  }

  public void setVeloxDFS(VeloxDFS _vdfs) { vdfs = _vdfs; }
  public void setFd(long _fd) { fd = _fd; }
  public void setFileSize(long fs) { fileSize = fs; }

  /**
   *    */
  @Override
  public synchronized long getPos() throws IOException {
    return mPos;
  }

  /** Velox likes things to be closed before it shuts down,
   * so closing the IOStream stuff voluntarily in a finalizer is good
   */
  @Override
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
      int remaining = 0;
      remaining = (int) (fileSize - mPos);

      return Math.max(remaining, Math.max(0, (int)remaining_bytes));
  }

  public synchronized void seek(long targetPos) throws IOException {
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
  //public synchronized int read() throws IOException {
  public int read() throws IOException {
    if (EOF) return -1;

    bufferOffset %= buffer.length;

    if (bufferOffset == 0) {
      bufferOffset = 0;
      remaining_bytes = read(getPos(), buffer, bufferOffset, buffer.length);
    }

    // If we could read bytes
    if (remaining_bytes > 0) {
        int value = (int)(buffer[bufferOffset] & 0xFF);

        bufferOffset++;
        //mPos = getPos() + 1;
        mPos++;
        remaining_bytes--;

        return value;
    } 

    return -1;
  }

  @Override
  public synchronized int read(long pos, byte[] buf, int off, int len)
    throws IOException {
    if (off < 0 || len < 0 || buf.length - off < len)
      throw new IndexOutOfBoundsException();

    if (len == 0) return 0;

    long readBytes = vdfs.read(fd, pos, buf, off, len); 
    total_read_bytes += readBytes;

    if (readBytes <= 0)
        EOF = true;

    return (int)readBytes;
  }

  @Override
  public void close() throws IOException {
    LOG.info("close for VeloxFSInputStream read_Bytes: " + total_read_bytes + " pos: " + getPos() + " remain: " + remaining_bytes );
    if (!closed) {
      vdfs.close(fd);

      closed = true;
      //seek(0);
      bufferOffset = 0;
    }
  }
}
