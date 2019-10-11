package pack.iscsi.io.direct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.spi.RandomAccessIO;

public class DirectIOTest {

  protected File _root = new File("./target/tmp/" + getClass().getName());

  @Before
  public void setup() {
    rmr(_root);
  }

  private void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  @Test
  public void testDirectIO() throws IOException {
    File file = new File(_root, UUID.randomUUID()
                                    .toString());
    file.getParentFile()
        .mkdirs();
    try (RandomAccessIO directIO = getRandomAccessIO(file)) {
      byte[] buffer = new byte[getBufferSize()];
      directIO.write(0, buffer);
    }
    assertEquals(getBufferSize(), file.length());
  }

  protected RandomAccessIO getRandomAccessIO(File file) throws IOException {
    return new DirectIO(file);
  }

  @Test
  public void testDirectIoRandomReadWriteTest() throws IOException {
    File file = new File(_root, UUID.randomUUID()
                                    .toString());
    file.getParentFile()
        .mkdirs();
    byte[] buffer = new byte[getBufferSize()];
    Random random = new Random(1);
    int blocks = getBlockCount();
    random.nextBytes(buffer);
    try (RandomAccessIO directIO = getRandomAccessIO(file)) {
      directIO.setLength(blocks * buffer.length);
      for (int i = 0; i < blocks; i++) {
        long position = buffer.length * i;
        directIO.write(position, buffer);
      }
    }
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      try (FileChannel channel = raf.getChannel()) {
        for (int i = 0; i < blocks; i++) {
          byte[] readBuffer = new byte[getBufferSize()];
          long position = i * readBuffer.length;
          ByteBuffer dst = ByteBuffer.wrap(readBuffer);
          while (dst.hasRemaining()) {
            position += channel.read(dst, position);
          }
          // for (int j = 0; j < buffer.length; j++) {
          // assertEquals("position=" + position + " index=" + j, buffer[j],
          // readBuffer[j]);
          // }
          assertTrue(Arrays.equals(buffer, readBuffer));
        }
      }
    }
    try (RandomAccessIO directIO = getRandomAccessIO(file)) {
      for (int i = 0; i < blocks; i++) {
        byte[] readBuffer = new byte[getBufferSize()];
        long position = i * readBuffer.length;
        directIO.read(position, readBuffer);
        // for (int j = 0; j < buffer.length; j++) {
        // assertEquals("position=" + position + " index=" + j, buffer[j],
        // readBuffer[j]);
        // }
        assertTrue(Arrays.equals(buffer, readBuffer));
      }
    }
    assertEquals(buffer.length * blocks, file.length());
  }

  @Test
  public void testDirectIoPerformanceBufferTest() throws IOException {
    File file = new File(_root, UUID.randomUUID()
                                    .toString());
    file.getParentFile()
        .mkdirs();
    byte[] buffer = new byte[getBufferSize()];
    Random random = new Random(1);
    int blocks = getBlockCount();
    int passes = 10000;
    random.nextBytes(buffer);
    for (int t = 0; t < 3; t++) {
      try (RandomAccessIO directIO = getRandomAccessIO(file)) {
        directIO.setLength(blocks * buffer.length);
        long start = System.nanoTime();
        for (int i = 0; i < passes; i++) {
          long position = buffer.length * random.nextInt(blocks);
          directIO.write(position, buffer);
        }
        long end = System.nanoTime();
        System.out.println((end - start) / 1_000_000.0 + " ms");
      }
    }
  }

  protected int getBlockCount() {
    return 100;
  }

  protected int getBufferSize() {
    return 100000;
  }

}
