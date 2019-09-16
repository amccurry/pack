package pack.iscsi.external;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.file.external.storage.LocalLogFileReaderWriter;
import pack.iscsi.file.external.storage.LocalLogFileReaderWriter.LocalLogReader;
import pack.iscsi.file.external.storage.LocalLogFileReaderWriter.LocalLogWriter;
import pack.iscsi.io.IOUtils;

public class LocalLogFileTest {

  private static final File DIR = new File("./target/tmp/LocalLogFileTest");

  @Before
  public void setup() {
    IOUtils.rmr(DIR);
    DIR.mkdirs();
  }

  @Test
  public void testLocalLogFileSingleEntry() throws IOException {
    byte[] bytes = new byte[] { 1, 2, 3 };
    try (LocalLogWriter writer = new LocalLogFileReaderWriter.LocalLogWriter(DIR)) {
      assertEquals(-1L, writer.getLastGeneration());
      writer.append(1234, 12345, bytes, 0, bytes.length);
      assertEquals(1234, writer.getLastGeneration());
    }

    File[] files = DIR.listFiles();
    for (File file : files) {
      try (LocalLogReader reader = new LocalLogFileReaderWriter.LocalLogReader(file)) {
        assertEquals(1234, reader.getMinGeneration());
        assertEquals(1234, reader.getMaxGeneration());
        reader.reset();
        if (reader.next()) {
          assertEquals(1234, reader.getGeneration());
          assertEquals(12345, reader.getPosition());
          assertEquals(3, reader.getLength());
          byte[] bs = reader.getBytes();
          assertTrue(Arrays.equals(bytes, bs));
        } else {
          fail();
        }
        assertFalse(reader.next());
      }
    }
  }

  @Test
  public void testLocalLogFileMultipleEntry() throws IOException {
    byte[] bytes = new byte[] { 1, 2, 3 };
    try (LocalLogWriter writer = new LocalLogFileReaderWriter.LocalLogWriter(DIR)) {
      assertEquals(-1L, writer.getLastGeneration());
      writer.append(1234, 12345, bytes, 0, bytes.length);
      assertEquals(1234, writer.getLastGeneration());
      writer.append(1235, 12345, bytes, 0, bytes.length);
      assertEquals(1235, writer.getLastGeneration());
    }

    File[] files = DIR.listFiles();
    for (File file : files) {
      try (LocalLogReader reader = new LocalLogFileReaderWriter.LocalLogReader(file)) {
        assertEquals(1234, reader.getMinGeneration());
        assertEquals(1235, reader.getMaxGeneration());
        reader.reset();
        if (reader.next()) {
          assertEquals(1234, reader.getGeneration());
          assertEquals(12345, reader.getPosition());
          assertEquals(3, reader.getLength());
          byte[] bs = reader.getBytes();
          assertTrue(Arrays.equals(bytes, bs));
        } else {
          fail();
        }
        if (reader.next()) {
          assertEquals(1235, reader.getGeneration());
          assertEquals(12345, reader.getPosition());
          assertEquals(3, reader.getLength());
          byte[] bs = reader.getBytes();
          assertTrue(Arrays.equals(bytes, bs));
        } else {
          fail();
        }
        assertFalse(reader.next());
      }
    }
  }

  @Test
  public void testLocalLogFileMultipleSort() throws IOException {
    byte[] bytes = new byte[] { 1, 2, 3 };
    long gen = 0;
    int passes = 50;
    for (int i = 0; i < passes; i++) {
      try (LocalLogWriter writer = new LocalLogFileReaderWriter.LocalLogWriter(DIR)) {
        writer.append(gen++, 12345, bytes, 0, bytes.length);
      }
    }

    File[] files = DIR.listFiles();
    List<LocalLogReader> readers = new ArrayList<>();
    for (File file : files) {
      readers.add(new LocalLogReader(file));
    }

    Collections.sort(readers);

    for (int i = 0; i < passes; i++) {
      LocalLogReader reader = readers.get(i);
      assertEquals(i, reader.getMinGeneration());
    }
  }

}
