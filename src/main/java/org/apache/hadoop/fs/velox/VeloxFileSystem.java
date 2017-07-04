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
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.URI;
import java.net.InetAddress;
import java.util.EnumSet;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.fs.FsStatus;

import org.apache.hadoop.fs.velox.VeloxInputStream;
import org.apache.hadoop.fs.velox.VeloxOutputStream;
import velox.VeloxDFS;
import velox.model.Metadata;
//import velox.Configuration;

public class VeloxFileSystem extends FileSystem {
  private static final Log LOG = LogFactory.getLog(VeloxFileSystem.class);
  private URI uri;

  private Path workingDir = "";
  private VeloxDFS vdfs = null;

  private velox.Configuration conf;

  /**
   * Create a new VeloxFileSystem.
   */
  public VeloxFileSystem() {
    conf = new velox.Configuration();
  }

  /**
   * Create an absolute path using the working directory.
   */
  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (vdfs == null) {
      this.vdfs = new VeloxDFS();
    }
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    //this.workingDir = getHomeDirectory();
    this.workingDir = "";
  }

  /** 
   * Open a file with a given its path
   * @param f File Path
   * @return VeloxInputStream
   */
  @Override
  public FSDataInputStream open(Path f) throws IOException {
    long fd = vdfs.open(f.toString());
    
    VeloxInputStream vis = new VeloxInputStream(vdfs, fd);
    return vis;
  }

  /**
   * Close down the FileSystem. Runs the base-class close method
   */
  @Override
  public void close() throws IOException {
    super.close(); // this method does stuff, make sure it's run!
  }

  /**
   * Get an FSDataOutputStream to append onto a file.
   * @param path The File you want to append onto
   * @param bufferSize Ceph does internal buffering but you can buffer in the Java code as well if you like.
   * @param progress The Progressable to report progress to.
   * Reporting is limited but exists.
   * @return An FSDataOutputStream that connects to the file on Ceph.
   * @throws IOException If the file cannot be found or appended to.
   */
  public FSDataOutputStream append(Path path, int bufferSize,
      Progressable progress) throws IOException {
    path = makeAbsolute(path);

    if (progress != null) {
      progress.progress();
    }

    long fd = vdfs.open(path.toString());

    if (progress != null) {
      progress.progress();
    }

    VeloxOutputStream vos = new VeloxOutputStream(vdfs, fd, bufferSize);
    return new FSDataOutputStream(vos, statistics);
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  /**
   * Create a directory and any nonexistent parents. Any portion
   * of the directory tree can exist without error.
   * @param path The directory path to create
   * @param perms The permissions to apply to the created directories.
   * @return true if successful, false otherwise
   * @throws IOException if the path is a child of a file.
   */
  @Override
  public boolean mkdirs(Path path, FsPermission perms) throws IOException {
    // Doesn't support
    return true;
  }

  /**
   * Create a directory and any nonexistent parents. Any portion
   * of the directory tree can exist without error. 
   * Apply umask from conf
   * @param f The directory path to create
   * @return true if successful, false otherwise
   * @throws IOException if the path is a child of a file.
   */
  @Override
  public boolean mkdirs(Path f) throws IOException {
    return mkdirs(f, FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf())));
  }

  /**
   * Get stat information on a file. 
   * @param path The path to stat.
   * @return FileStatus object containing the stat information.
   */
  public FileStatus getFileStatus(Path path) throws IOException {
    path = makeAbsolute(path);

    long fd = vdfs.open(path.toString());
    Metadata data = vdfs.getMetadata(fd);

    return new FileStatus(data.size, false, data.replica, conf.blockSize(), 0, path);
  }

  /**
   * Get the FileStatus for each listing in a directory.
   * @param path The directory to get listings from.
   * @return FileStatus[] containing one FileStatus for each directory listing;
   *         null if path does not exist.
   */
  public FileStatus[] listStatus(Path path) throws IOException {
    path = makeAbsolute(path);

    // TODO: implementation using ls function of vdfs

    return new FileStatus[];
  }

  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    // Doesn't support
  }

  @Override
  public void setTimes(Path path, long mtime, long atime) throws IOException {
    // Doesn't support
  }
  /**
   * Create a new file and open an FSDataOutputStream that's connected to it.
   * @param path The file to create.
   * @param permission The permissions to apply to the file.
   * @param overwrite If true, overwrite any existing file with
	 * this name; otherwise don't.
   * @param bufferSize Ceph does internal buffering, but you can buffer
   *   in the Java code too if you like.
   * @param replication Replication factor. See documentation on the
   *   "ceph.data.pools" configuration option.
   * @param blockSize Ignored by Ceph. You can set client-wide block sizes
   * via the fs.ceph.blockSize param if you like.
   * @param progress A Progressable to report back to.
   * Reporting is limited but exists.
   * @return An FSDataOutputStream pointing to the created file.
   * @throws IOException if the path is an
   * existing directory, or the path exists but overwrite is false, or there is a
   * failure in attempting to open for append with Ceph.
   */
  public FSDataOutputStream create(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    path = makeAbsolute(path);

    boolean exists = exists(path);

    if (progress != null) {
      progress.progress();
    }

    if (exists) {
      if (overwrite) {
        // TODO: overwrite
      }
      else
        throw new FileAlreadyExistsException();
    } 

    if (progress != null) {
      progress.progress();
    }

    /* Sanity check. Ceph interface uses int for striping strategy */
    if (blockSize > Integer.MAX_VALUE) {
      blockSize = Integer.MAX_VALUE;
      LOG.info("blockSize too large. Rounding down to " + blockSize);
    }
    else if (blockSize <= 0)
      throw new IllegalArgumentException("Invalid block size: " + blockSize);

    long fd = vdfs.open(path.toString());

    if (progress != null) {
      progress.progress();
    }

    VeloxOutputStream vos = new VeloxOutputStream(vdfs, fd, bufferSize);
    return new FSDataOutputStream(vos, statistics);
  }

  /**
  * Opens an FSDataOutputStream at the indicated Path with write-progress
  * reporting. Same as create(), except fails if parent directory doesn't
  * already exist.
  * @param path the file name to open
  * @param permission
  * @param overwrite if a file with this name already exists, then if true,
  * the file will be overwritten, and if false an error will be thrown.
  * @param bufferSize the size of the buffer to be used.
  * @param replication required block replication for the file.
  * @param blockSize
  * @param progress
  * @throws IOException
  * @see #setPermission(Path, FsPermission)
  * @deprecated API only for 0.20-append
  */
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    path = makeAbsolute(path);

    return this.create(path, permission, overwrite,
        bufferSize, replication, blockSize, progress);
  }

  /**
   * Rename a file or directory.
   * @param src The current path of the file/directory
   * @param dst The new name for the path.
   * @return true if the rename succeeded, false otherwise.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    src = makeAbsolute(src);
    dst = makeAbsolute(dst);

    // TODO: rename NOT implemented

    return true;
  }

  /**
   * Get a BlockLocation object for each block in a file.
   *
   * @param file A FileStatus object corresponding to the file you want locations for.
   * @param start The offset of the first part of the file you are interested in.
   * @param len The amount of the file past the offset you are interested in.
   * @return A BlockLocation[] where each object corresponds to a block within
   * the given range.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    Path path = makeAbsolute(file.getPath());

    long fd = vdfs.open(path.toString());

    Metadata data = vdfs.getMetadata(fd);

    ArrayList<BlockLocation> blocks = new ArrayList<BlockLocation>();

    long curPos = start;
    long endOff = curPos + len;

    // Prototype of BlockLocation
    // BlockLocation(String[] names, String[] hosts, long offset, long length)
    // TODO: getting hosts of replicas 
    for(int i=0; i<numBlock; i++) {
      BlockMetadata bdata = data.blocks[i];
      String[] names = new String[1];
      names[0] = bdata.name;

      String[] hosts = new String[1];
      hosts[0] = bdata.host;
       
      blocks.add(new BlockLocation(names, hosts, curPos, bdata.size));
      curPos += bdata.size;
    }

    BlockLocation[] locations = new BlockLocation[blocks.size()];
    locations = blocks.toArray(locations);

    return locations;
  }

  @Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, false);
	}

  public boolean delete(Path path, boolean recursive) throws IOException {
    path = makeAbsolute(path);
    return vdfs.remove(path.toString());
  }

  @Override
  public short getDefaultReplication() {
    return conf.numOfReplications();
  }

  @Override
  public long getDefaultBlockSize() {
    return conf.blockSize();
  }
  
  @Override
  public FsStatus getStatus(Path p) throws IOException {
    // TODO: not implemented yet
	  return new FsStatus(0, 0, 0);
  }

  }
