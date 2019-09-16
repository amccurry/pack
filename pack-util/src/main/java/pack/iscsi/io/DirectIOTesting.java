package pack.iscsi.io;

public class DirectIOTesting {

  // public static void main(String[] args) throws IOException {
  // Random random = new Random();
  // byte[] buffer = new byte[4096];
  // random.nextBytes(buffer);
  // for (int bufferSize = 4096; bufferSize < 150000; bufferSize += 4096) {
  // File file = new File("./nocachefile");
  // file.delete();
  // long length = Integer.MAX_VALUE;
  // try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
  // raf.setLength(length);
  // }
  // long s = System.nanoTime();
  // write(buffer, file, bufferSize);
  // long e = System.nanoTime();
  // System.out.println(bufferSize + " " + (e - s) / 1_000_000.0);
  // }
  // }
  //
  // private static void write(byte[] buffer, File file, int bufferSize) throws
  // IOException {
  // long length = file.length();
  // long position = 0;
  // try (FileIO fileIO = FileIO.open(file, bufferSize)) {
  // while (length > 0) {
  // fileIO.writeFully(position, buffer);
  // position += buffer.length;
  // length -= buffer.length;
  // }
  // }
  // }

}
