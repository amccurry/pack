package pack.block.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileCounter {

  public static void main(String[] args) throws IOException {
    FileCounter fileCounter = new FileCounter(new File("./testfile"));
    System.out.println(fileCounter.getValue());
    fileCounter.inc();
    System.out.println(fileCounter.getValue());
    fileCounter.dec();
    System.out.println(fileCounter.getValue());
  }

  private final File _file;

  public FileCounter(File file) {
    _file = file;
  }

  public synchronized void dec() throws FileNotFoundException, IOException {
    try (RandomAccessFile rand = new RandomAccessFile(_file, "rw")) {
      rand.setLength(8);
      long l = rand.readLong();
      rand.seek(0);
      rand.writeLong(l - 1);
    }
  }

  public synchronized long getValue() throws IOException {
    try (RandomAccessFile rand = new RandomAccessFile(_file, "rw")) {
      rand.setLength(8);
      return rand.readLong();
    }
  }

  public synchronized void inc() throws IOException {
    try (RandomAccessFile rand = new RandomAccessFile(_file, "rw")) {
      rand.setLength(8);
      long l = rand.readLong();
      rand.seek(0);
      rand.writeLong(l + 1);
    }
  }

}
