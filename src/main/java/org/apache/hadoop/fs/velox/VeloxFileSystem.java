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
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.fs.velox.VeloxFSInputStream;
import org.apache.hadoop.fs.velox.VeloxFSOutputStream;
import com.dicl.velox.VeloxDFS;
import com.dicl.velox.model.Metadata;
import com.dicl.velox.model.BlockMetadata;
//import velox.Configuration;


//import java.net.InetAddress;
//import java.net.UnknownHostException;

public class VeloxFileSystem extends FileSystem {
  private static final Log LOG = LogFactory.getLog(VeloxFileSystem.class);

  public static final String VELOX_URI_SCHEME = "velox";
  public static final URI NAME = URI.create(VELOX_URI_SCHEME + ":///");

  private Path workingDir;
  private VeloxDFS veloxdfs;

  private com.dicl.velox.Configuration veloxConf;

  private UserGroupInformation ugi;

  private FsPermission defaultFilePermission;// = FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(getConf()));
  private FsPermission defaultDirPermission;// = FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf()));

  /**
   * Create a new VeloxFileSystem.
   */
  public VeloxFileSystem() {
  }

  /**
   * Create an absolute path using the working directory.
   * It is not an absolute path because velox doesn't support directory.
   */
  private Path makeVeloxPath(Path path) {
    if(path == null) 
      return workingDir;

    //return path;
    Path ret = Path.getPathWithoutSchemeAndAuthority(path);
    return ret.toString().startsWith("/") ? ret : new Path("/" + ret.toString());

    //return new Path(Path.SEPARATOR + path.getName());
    
    /*
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
    */
  }

  public URI getUri() {
    return NAME;
  }

  @Override
  public String getScheme() {
    return VELOX_URI_SCHEME;
  }

  @Override
  public void initialize(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (veloxdfs == null) {
      this.veloxdfs = new VeloxDFS();
    }
    setConf(conf);
    this.workingDir = new Path("/");
    this.ugi = UserGroupInformation.getCurrentUser();
    //
    if(this.veloxConf == null)
      this.veloxConf = new com.dicl.velox.Configuration(conf.get("fs.velox.json"));
    LOG.info("initialize with " + this.workingDir.toString() + " for " + this.ugi.getUserName());

    this.defaultFilePermission = FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(getConf()));
    this.defaultDirPermission = FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf()));
  }

  /** 
   * Open a file with a given its path
   * @param f File Path
   * @return VeloxFSInputStream
   */
  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return open(f, getConf().getInt("fs.velox.inputstream.buffersize", 8388608));
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    f = makeVeloxPath(f);
    LOG.info("open with " + f.toString());

    long fd = veloxdfs.open(f.toString());
    Metadata md = veloxdfs.getMetadata(fd);

    return new FSDataInputStream(new VeloxFSInputStream(veloxdfs, fd, bufferSize, md.size));
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
   * @param bufferSize Velox does internal buffering but you can buffer in the Java code as well if you like.
   * @param progress The Progressable to report progress to.
   * Reporting is limited but exists.
   * @return An FSDataOutputStream that connects to the file on Velox.
   * @throws IOException If the file cannot be found or appended to.
   */
  public FSDataOutputStream append(Path path, int bufferSize,
      Progressable progress) throws IOException {
    path = makeVeloxPath(path);

    if (progress != null) {
      progress.progress();
    }

    long fd = veloxdfs.open(path.toString());

    if (progress != null) {
      progress.progress();
    }

    return new FSDataOutputStream(new VeloxFSOutputStream(veloxdfs, fd, bufferSize), statistics);
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeVeloxPath(dir);
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
    LOG.info("mkdir with " + path.toString());

    path = makeVeloxPath(path);

    if(path == null) return false;

    if(path.getParent() != null && !path.isRoot())
      mkdirs(path.getParent(), perms);

    long fd = veloxdfs.open(path.toString());
    veloxdfs.close(fd);

/*
    Path p = path;
    while(p != null && !p.isRoot()) {
      LOG.info(p.toString());
      long fd = veloxdfs.open(p.toString());
      veloxdfs.close(fd);
      p = p.getParent();
    }
    */

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
    return mkdirs(f, this.defaultDirPermission);
  }

  /**
   * Get stat information on a file. 
   * @param path The path to stat.
   * @return FileStatus object containing the stat information.
   */
  public FileStatus getFileStatus(Path path) throws IOException {
    LOG.info("getFileStatus with " + path.toString());
    Path originalPath = path;
    path = makeVeloxPath(path);

    if(!veloxdfs.exists(path.toString())) 
      throw new FileNotFoundException();

    long fd = veloxdfs.open(path.toString());
    Metadata data = veloxdfs.getMetadata(fd);

    FileStatus ret = new FileStatus(data.size, false, data.replica, veloxConf.blockSize(), 0, 0,
      (data.size == 0 ? this.defaultDirPermission : this.defaultFilePermission), 
      this.ugi.getUserName(), this.ugi.getPrimaryGroupName(), makeQualified(path));
    return ret;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    LOG.info("listStatus with " + path.toString());

// try InetAddress.LocalHost first;
//      NOTE -- InetAddress.getLocalHost().getHostName() will not work in certain environments.
/*
try {
    String result = InetAddress.getLocalHost().getHostName();
    LOG.info(result);
} catch (UnknownHostException e) {
    // failed;  try alternate means.
}
*/

    path = makeVeloxPath(path);

    if(!veloxdfs.exists(path.toString()))
      throw new FileNotFoundException();

    Metadata[] metadata = veloxdfs.list(false, path.toString());

    FileStatus[] list = new FileStatus[metadata.length];
    for(int i=0; i<metadata.length; i++) {
      Metadata m = metadata[i];
      list[i] = new FileStatus(m.size, false, m.replica, veloxConf.blockSize(), 0, 0, 
        (m.size == 0 ? this.defaultDirPermission : this.defaultFilePermission), 
        this.ugi.getUserName(), this.ugi.getPrimaryGroupName(), makeQualified(new Path(path, m.name)));
    }

    return list;
  }

  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    // Doesn't support
  }

  @Override 
  public void setOwner(Path path, String username, String groupname) throws IOException {
    // Doesn't support
    LOG.info("setOwner with " + path + ", " + username + ", " + groupname);
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
   * @param bufferSize Velox does internal buffering, but you can buffer
   *   in the Java code too if you like.
   * @param replication Replication factor. See documentation on the
   *   "ceph.data.pools" configuration option.
   * @param blockSize Ignored by Velox. You can set client-wide block sizes
   * via the fs.ceph.blockSize param if you like.
   * @param progress A Progressable to report back to.
   * Reporting is limited but exists.
   * @return An FSDataOutputStream pointing to the created file.
   * @throws IOException if the path is an
   * existing directory, or the path exists but overwrite is false, or there is a
   * failure in attempting to open for append with Velox.
   */
  public FSDataOutputStream create(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {

    LOG.info("create with " + path.toString());
    path = makeVeloxPath(path);


    if (progress != null) {
      progress.progress();
    }

    if (exists(path)) {
      if (overwrite) {
        // TODO: overwrite
      }
      else
        throw new FileAlreadyExistsException();
    } 

    if (progress != null) {
      progress.progress();
    }

    /* Sanity check. Velox interface uses int for striping strategy */
    if (blockSize > Integer.MAX_VALUE) {
      blockSize = Integer.MAX_VALUE;
      LOG.info("blockSize too large. Rounding down to " + blockSize);
    }
    else if (blockSize <= 0)
      throw new IllegalArgumentException("Invalid block size: " + blockSize);

    long fd = veloxdfs.open(path.toString());

    if (progress != null) {
      progress.progress();
    }

    return new FSDataOutputStream(new VeloxFSOutputStream(veloxdfs, fd, bufferSize), statistics);
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

    path = makeVeloxPath(path);

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
    LOG.info("rename with " + src.toString() + ", " + dst.toString());
    src = makeVeloxPath(src);
    dst = makeVeloxPath(dst);


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
    LOG.info("getFileBlockLocations with " + file.getPath().toString() + ", " + String.valueOf(start) + ", " + String.valueOf(len) + ", " + file.getLen());

    if (file.getLen() <= start) {
      return new BlockLocation[0];
    }

    Path path = makeVeloxPath(file.getPath());

    long fd = veloxdfs.open(path.toString());

    Metadata data = veloxdfs.getMetadata(fd);
    LOG.info("The # of blocks : " + data.numBlock);

    long curPos = start;
    long endOff = curPos + len;

    BlockLocation[] locations = new BlockLocation[data.numBlock];

    // Prototype of BlockLocation
    // BlockLocation(String[] names, String[] hosts, long offset, long length)
    // TODO: getting hosts of replicas 
    for(int i=0; i<data.numBlock; i++) {
      LOG.info(new Path(data.blocks[i].name).getName().toString() + ", " + data.blocks[i].host + ", " + String.valueOf(curPos) + ", " + String.valueOf(data.blocks[i].size));
      locations[i] = new BlockLocation(
        //new String[]{new Path(veloxConf.storagePath(), data.blocks[i].name).toString()}, 
        new String[]{data.blocks[i].name},
        new String[]{data.blocks[i].host}, 
        curPos, data.blocks[i].size
      );
      curPos += data.blocks[i].size;
    }

    return locations;
  }

  @Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, false);
	}

  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      LOG.info("delete with " + path.toString() + ", " + String.valueOf(recursive));
      path = makeVeloxPath(path);

      if(recursive) {
        FileStatus[] files = listStatus(path);
        for(int i=0; i<files.length; i++) 
          delete(files[i].getPath(), false);
      }
      veloxdfs.remove(path.toString());
    } catch(FileNotFoundException e) {
      return false;
    }
    return true;
  }

  @Override
  public short getDefaultReplication() {
    LOG.info("getDefaultReplication()");
    return (short)veloxConf.numOfReplications();
  }

  @Override
  public long getDefaultBlockSize() {
    LOG.info("getDefaultBlockSize()");
    return veloxConf.blockSize();
  }
  
  @Override
  public FsStatus getStatus(Path p) throws IOException {
    // TODO: not implemented yet
	  return new FsStatus(0, 0, 0);
  }

  /** Check if exists.
   ** @param f source file
   **/
  @Override
  public boolean exists(Path f) throws IOException {
    LOG.info("exists with " + f.toString());
    try {
      return getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }
}
