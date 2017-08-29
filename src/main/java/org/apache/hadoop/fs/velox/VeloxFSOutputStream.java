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

import com.dicl.velox.VeloxDFS;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.util.Progressable;
//import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * <p>
 * An {@link OutputStream} for a VeloxFileSystem and corresponding
 * Velox instance.
 */
public class VeloxFSOutputStream extends OutputStream {
  private static final Log LOG = LogFactory.getLog(VeloxFSOutputStream.class);

  private boolean closed;

  private VeloxDFS vdfs;
  private long fd;
  private int mPos = 0; // offset of the file

  private byte[] buffer;
  private int bufUsed = 0; // offset of the buffer

  public VeloxFSOutputStream() {
    super();
    closed = false;
  }

  public VeloxFSOutputStream(VeloxDFS _vdfs, long _fd, int bufferSize) {
    super();
    closed = false;
    this.vdfs = _vdfs;
    this.fd = _fd;
    this.buffer = new byte[bufferSize];
  }

  public void setVeloxDFS(VeloxDFS _vdfs) { vdfs = _vdfs; }
  public void setFd(long _fd) { fd = _fd; }
  public void setBuffer(int bufferSize) {
    buffer = new byte[bufferSize];
  }

  protected void finalize() throws Throwable {
    try {
      if (!closed) {
        close();
      }
    } finally {
      super.finalize();
    }
  }

  private synchronized void checkOpen() throws IOException {
    if (closed)
      throw new IOException("operation on closed stream (fd=" + fd + ")");
  }

  public synchronized int getPos() {
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
        flush();
    }
  }
   
  @Override
  public synchronized void flush() throws IOException {
    if (bufUsed == 0)
      return;
      
    long ret = vdfs.write(fd, getPos(), buffer, 0, bufUsed);
    if (ret < 0)
      throw new IOException("vdfs.write: ret=" + ret);

    mPos = getPos() + bufUsed;
    bufUsed = 0;
  }
  
  @Override
  public synchronized void close() throws IOException {
    //checkOpen();
    flush();
    vdfs.close(fd);
    closed = true;
  }
}
