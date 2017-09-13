/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pack.block.blockstore.hdfs.kvs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Ordering;

import pack.block.blockstore.hdfs.HdfsMiniClusterUtil;
import pack.block.blockstore.hdfs.kvs.HdfsKeyValueStore;

public class HdfsKeyValueStoreTest {

  private static final File TMPDIR = new File(System.getProperty("hdfs.tmp.dir", "./target/tmp_HdfsKeyValueStoreTest"));

  private static Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;

  private static Timer _timer;
  private Path _path;

  @BeforeClass
  public static void startCluster() {
    _cluster = HdfsMiniClusterUtil.startDfs(_configuration, true, TMPDIR.getAbsolutePath());
    _timer = new Timer("IndexImporter", true);
  }

  @AfterClass
  public static void stopCluster() {
    _timer.cancel();
    _timer.purge();
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  @Before
  public void setup() throws IOException {
    FileSystem fileSystem = _cluster.getFileSystem();
    _path = new Path("/test").makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    fileSystem.delete(_path, true);
  }

  @Test
  public void testPutGet() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    store.put(1, toBytesBuffer("value1"));
    store.put(2, toBytesBuffer("value2"));
    store.sync(true);
    ByteBuffer value = createAndPopulateByteBuffer(6);
    assertTrue(store.get(1, value));
    assertEquals(toBytesBuffer("value1"), value);
    assertTrue(store.get(2, value));
    assertEquals(toBytesBuffer("value2"), value);
    store.close();
  }

  @Test
  public void testPutGetDelete() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    store.put(1, toBytesBuffer("value1"));
    store.put(2, toBytesBuffer("value2"));
    store.sync(true);
    ByteBuffer value = createAndPopulateByteBuffer(6);
    store.get(1, value);
    assertEquals(toBytesBuffer("value1"), value);
    store.get(2, value);
    assertEquals(toBytesBuffer("value2"), value);

    store.delete(2);
    store.sync(true);
    assertFalse(store.get(2, value));
    store.close();
  }

  @Test
  public void testPutGetReopen() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    store1.put(1, toBytesBuffer("value1"));
    store1.put(2, toBytesBuffer("value2"));
    store1.sync(true);
    ByteBuffer value1 = createAndPopulateByteBuffer(6);
    store1.get(1, value1);
    assertEquals(toBytesBuffer("value1"), value1);
    store1.get(2, value1);
    assertEquals(toBytesBuffer("value2"), value1);
    store1.close();

    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    ByteBuffer value2 = createAndPopulateByteBuffer(6);
    store2.get(1, value2);
    assertEquals(toBytesBuffer("value1"), value2);
    store2.get(2, value2);
    assertEquals(toBytesBuffer("value2"), value2);
    store2.close();
  }

  @Test
  public void testFileRolling() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path, 1000);
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    store.put(1, createAndPopulateByteBuffer(0));
    assertEquals(1, fileSystem.listStatus(_path).length);
    store.put(1, createAndPopulateByteBuffer(2000));
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.close();
  }

  private ByteBuffer createAndPopulateByteBuffer(int size) {
    byte[] buf = new byte[size];
    Arrays.fill(buf, (byte) 1);
    return ByteBuffer.wrap(buf);
  }

  @Test
  public void testFileGC() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path, 1000);
    store.put(1, createAndPopulateByteBuffer(0));
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    assertEquals(1, fileSystem.listStatus(_path).length);
    store.put(1, createAndPopulateByteBuffer(2000));
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.put(1, createAndPopulateByteBuffer(2000));
    store.cleanupOldFiles();
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.close();
  }

  @Test
  public void testTwoKeyStoreInstancesWritingAtTheSameTime() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    listFiles();
    store1.put(1, createAndPopulateByteBuffer(2000));
    listFiles();
    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    listFiles();
    store2.put(1, createAndPopulateByteBuffer(1000));
    listFiles();
    store1.put(2, createAndPopulateByteBuffer(2000));
    listFiles();
    store2.put(2, createAndPopulateByteBuffer(1000));
    listFiles();
    store1.put(3, createAndPopulateByteBuffer(2000));
    listFiles();
    store2.put(3, createAndPopulateByteBuffer(1000));
    listFiles();
    try {
      store1.sync(true);
      fail();
    } catch (Exception e) {

    }
    store2.sync(true);

    try {
      store1.close();
      fail();
    } catch (Exception e) {

    }

    store2.close();

    HdfsKeyValueStore store3 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    Iterable<Entry<Long, ByteBuffer>> scan = store3.scan(null);
    for (Entry<Long, ByteBuffer> e : scan) {
      System.out.println(e.getValue()
                          .capacity());
    }
    store3.close();
  }

  @Test
  public void testTwoKeyStoreInstancesWritingAtTheSameTimeSmallFiles() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path, 1000);
    store1.put(1, createAndPopulateByteBuffer(2000));
    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path, 1000);
    store2.put(1, createAndPopulateByteBuffer(1000));
    try {
      store1.put(2, createAndPopulateByteBuffer(2000));
      fail();
    } catch (Exception e) {
      // Should throw exception
      store1.close();
    }
    store2.put(2, createAndPopulateByteBuffer(1000));
    store2.put(3, createAndPopulateByteBuffer(1000));
    store2.sync(true);
    store2.close();

    HdfsKeyValueStore store3 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path);
    Iterable<Entry<Long, ByteBuffer>> scan = store3.scan(null);
    for (Entry<Long, ByteBuffer> e : scan) {
      System.out.println(e.getValue()
                          .capacity());
    }
    store3.close();
  }

  @Test
  public void testReadonlyPut() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path, 1000);
    store1.put(1, createAndPopulateByteBuffer(2000));

    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_path.getName(), true, _timer, _configuration, _path, 1000);
    assertTrue(store2.get(1, createAndPopulateByteBuffer(2000)));

    try {
      store2.put(1, createAndPopulateByteBuffer(2000));
      fail();
    } catch (IOException e) {

    }

    try {
      store2.delete(1);
      fail();
    } catch (IOException e) {

    }

    try {
      store2.sync(true);
      fail();
    } catch (IOException e) {

    }

    // Store 1 should still be able to write.
    store1.put(2, createAndPopulateByteBuffer(2000));

    // Store 2 should not be able to find.
    assertFalse(store2.get(2, createAndPopulateByteBuffer(2000)));

    store2.close();
    store1.close();

  }

  @Test
  public void testExternalWrite() throws IOException {
    try (
        HdfsKeyValueStore store1 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path, 1000)) {
      Random random = new Random();
      List<Long> ids = new ArrayList<Long>();

      ByteBuffer buffer = createAndPopulateByteBuffer(20);
      for (int i = 0; i < 1000; i++) {
        long id = random.nextLong();
        store1.put(id, buffer);
        ids.add(id);
      }

      List<Long> idsOutput = new ArrayList<Long>();
      store1.writeExternal((key, writable) -> {
        System.out.println(key);
        idsOutput.add(key);
      }, true);
      assertTrue(Ordering.natural()
                         .isOrdered(idsOutput));

      for (Long id : ids) {
        assertFalse(store1.get(id, buffer));
      }

    }
  }

  @Test
  public void testExternalWriteWithRace() throws IOException {
    try (
        HdfsKeyValueStore store1 = new HdfsKeyValueStore(_path.getName(), false, _timer, _configuration, _path, 1000)) {
      Random random = new Random();
      List<Long> ids = new ArrayList<Long>();
      ByteBuffer buffer = createAndPopulateByteBuffer(20);
      for (int i = 0; i < 3; i++) {
        long id = random.nextLong();
        store1.put(id, buffer);
        ids.add(id);
      }
      AtomicReference<Long> ref = new AtomicReference<>();
      List<Long> idsOutput = new ArrayList<Long>();
      store1.writeExternal((key, writable) -> {
        if (ref.get() == null) {
          ref.set(key);
          store1.put(key, createAndPopulateByteBuffer(20));
        }
        idsOutput.add(key);
      }, true);
      assertTrue(Ordering.natural()
                         .isOrdered(idsOutput));
      for (Long id : ids) {
        buffer.clear();
        if (ref.get()
               .equals(id)) {
          assertTrue(store1.get(id, buffer));
        } else {
          assertFalse(store1.get(id, buffer));
        }
      }
    }
  }

  private void listFiles() throws IOException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    for (FileStatus status : fileSystem.listStatus(_path)) {
      System.out.println(status.getPath() + " " + status.getLen());
    }
  }

  private ByteBuffer toBytesBuffer(String s) {
    return ByteBuffer.wrap(s.getBytes());
  }

}