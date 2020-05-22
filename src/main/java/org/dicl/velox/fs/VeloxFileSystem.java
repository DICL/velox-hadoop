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

import org.dicl.velox.fs.VeloxFSInputStream;
import org.dicl.velox.fs.VeloxFSOutputStream;
import com.dicl.velox.VeloxDFS;
import com.dicl.velox.model.Metadata;
import com.dicl.velox.model.BlockMetadata;

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
      this.veloxdfs = new VeloxDFS(null,0,true);
    }
    setConf(conf);
    this.workingDir = new Path("/");
    this.ugi = UserGroupInformation.getCurrentUser();

    if(this.veloxConf == null)
      this.veloxConf = new com.dicl.velox.Configuration(conf.get("fs.velox.json"));

    LOG.debug("initialize with " + this.workingDir.toString() + " for " + this.ugi.getUserName());

    this.defaultFilePermission = FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(getConf()));
    this.defaultDirPermission = FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf()));

    long fd = veloxdfs.open("/");
    veloxdfs.close(fd);
  }

  /**
   * Open a file with a given its path
   * @param f File Path
   * @return VeloxFSInputStream
   */
  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return open(f, getConf().getInt("fs.velox.inputstream.buffersize", 16777216));
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    f = makeVeloxPath(f);
    LOG.info("open with " + f.toString());

    long fd = veloxdfs.open(f.toString());
    Metadata md = veloxdfs.getMetadata(fd, (byte)0);

    LOG.info("Finish open"); 

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
    LOG.debug("mkdir with " + path.toString());

    path = makeVeloxPath(path);

    try {
      final FileStatus stat = getFileStatus(path.getParent());
      if (!stat.isDirectory()) {
        throw new IOException("parent is not a dir: " + path.getParent().toString());
      }
    } catch(FileNotFoundException e) {
      mkdirs(path.getParent(), perms);
    }

    long fd = veloxdfs.open(path.toString());
    veloxdfs.close(fd);

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
    LOG.debug("getFileStatus with " + path.toString());
    Path originalPath = path;
    path = makeVeloxPath(path);

    if(!veloxdfs.exists(path.toString())) {
      throw new FileNotFoundException();
    }

    long fd = veloxdfs.open(path.toString());
    Metadata data = veloxdfs.getMetadata(fd, (byte)2);

    //long blockSize = (data.blocks == null || data.size == 0) ? veloxConf.blockSize() : (long) ((double) data.size / data.blocks.length);
    long blockSize = (long) (((double) data.size / data.numBlock) + 0.5);

    return new FileStatus(data.size, (data.size == 0 || "/".equals(data.name)), data.replica, blockSize, 0, 0,
      null, this.ugi.getUserName(), this.ugi.getPrimaryGroupName(), makeQualified(path));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    LOG.debug("listStatus with " + path.toString());

    path = makeVeloxPath(path);

    if(!veloxdfs.exists(path.toString()))
      throw new FileNotFoundException();

    Metadata[] metadata = veloxdfs.list(false, path.toString());

    FileStatus[] list = new FileStatus[metadata.length];
    for(int i=0; i<metadata.length; i++) {
      Metadata m = metadata[i];
      //long blockSize = (m.blocks == null || m.size == 0) ? veloxConf.blockSize() : (long) ((double) m.size / m.numBlock);
      long blockSize = (long) (((double) m.size / m.numBlock) + 0.5);
      list[i] = new FileStatus(m.size, (m.size == 0 || "/".equals(m.name)), m.replica, blockSize, 0, 0,
        null, this.ugi.getUserName(), this.ugi.getPrimaryGroupName(), makeQualified(new Path(path, m.name)));
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
    LOG.debug("setOwner with " + path + ", " + username + ", " + groupname);
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

    LOG.debug("create with " + path.toString());
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
      LOG.debug("blockSize too large. Rounding down to " + blockSize);
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
    LOG.debug("rename with " + src.toString() + ", " + dst.toString());
    src = makeVeloxPath(src);
    dst = makeVeloxPath(dst);

    if(!veloxdfs.exists(src.toString()))
      throw new IOException(src.toString() + " doesn't exist.");

    if(!veloxdfs.rename(src.toString(), dst.toString()))
      throw new IOException("failed to rename " + src.toString() + " to " + dst.toString());

    return true;
/*
    long fd = veloxdfs.open(src.toString());
    Metadata md = veloxdfs.getMetadata(fd, (byte)0);
    LOG.debug("Size of " + src.toString() + " is " + md.size);
    byte[] buf = new byte[(int)md.size + 1];

    long readBytes = veloxdfs.read(fd, 0, buf, 0, md.size);

    long dst_fd = veloxdfs.open(dst.toString());
    veloxdfs.write(dst_fd, 0, buf, 0, md.size);

    delete(src);

    // TODO: rename NOT implemented

    return true;
*/
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
    LOG.debug("getFileBlockLocations with " + file.getPath().toString() + ", " + String.valueOf(start) + ", " + String.valueOf(len) + ", " + file.getLen());

    if (file.getLen() <= start) {
      return new BlockLocation[0];
    }

    Path path = makeVeloxPath(file.getPath());

    long fd = veloxdfs.open(path.toString());

    Metadata data = veloxdfs.getMetadata(fd, (byte)3);
    LOG.debug("The # of blocks : " + data.numBlock);

    long curPos = start;
    long endOff = curPos + len;

    BlockLocation[] locations = new BlockLocation[data.numBlock];

    // Prototype of BlockLocation
    for(int i=0; i<data.numBlock; i++) {
      LOG.debug(new Path(data.blocks[i].name).getName().toString() + ", " + data.blocks[i].host + ", " + String.valueOf(curPos) + ", " + String.valueOf(data.blocks[i].size));

      locations[i] = new BlockLocation(
              new String[]{data.blocks[i].name},
              new String[]{data.blocks[i].host},
              curPos, data.blocks[i].size);
      curPos += data.blocks[i].size;
    }

    return locations;
  }

  @Deprecated
    public boolean delete(Path path) throws IOException {
        return delete(path, false);
    }

  public boolean delete(Path path, boolean recursive) throws IOException {
      LOG.debug("delete with " + path.toString() + ", " + String.valueOf(recursive));
    return true;
/*
    LOG.debug("delete with " + path.toString() + ", " + String.valueOf(recursive));

    try {
      path = makeVeloxPath(path);
      getFileStatus(path);

      if(recursive) {
        FileStatus[] files = listStatus(path);
        for(int i=0; i<files.length; i++)
          delete(files[i].getPath(), false);
      }
      //if (recursive == false)
        veloxdfs.remove(path.toString());
    } catch(FileNotFoundException e) {
      LOG.debug("I Am being compiled again");
      return false;
    }
    return true;
*/
  }

  @Override
  public short getDefaultReplication() {
    LOG.debug("getDefaultReplication()");
    return (short)veloxConf.numOfReplications();
  }

  @Override
  public long getDefaultBlockSize() {
    LOG.debug("getDefaultBlockSize()");
    return veloxConf.blockSize();
  }

/*
  @Override
  public long getDefaultBlockSize(Path p) {
    LOG.debug("getDefaultBlockSize(Path " + p.toString() + ")");

    try {
      if(!exists(p))
        return getDefaultBlockSize();
    }
    catch (IOException e) {
        return getDefaultBlockSize();
    }

    p = makeVeloxPath(p);

    long fd = veloxdfs.open(p.toString());

    Metadata md = veloxdfs.getMetadata(fd, (byte)2);
    return (long) Math.ceil(md.size / md.blocks.length);
  }
  */

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
    if(f == null) return false;

    LOG.debug("exists with " + f.toString());
    try {
      return getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }
}
