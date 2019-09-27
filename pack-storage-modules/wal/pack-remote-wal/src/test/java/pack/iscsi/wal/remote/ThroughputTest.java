package pack.iscsi.wal.remote;

import java.util.Random;

import pack.iscsi.wal.remote.RemoteWALClient.RemoteWALClientConfig;

public class ThroughputTest {

  public static void main(String[] args) throws Exception {

    RemoteWALClientConfig config = RemoteWALClientConfig.builder()
                                                                            .hostname("localhost")
                                                                            .port(43897)
                                                                            .build();

    try (RemoteWALClient client = new RemoteWALClient(config)) {
      Random random = new Random();
      byte[] buffer = new byte[8192];
      long volumeId = random.nextInt(10);
      long blockId = random.nextInt(10);
      long generation = 0;

      long bytesSent = 0;
      long totalTime = 0;
      long start = System.nanoTime();
      long count = 0;
      while (true) {
        if (start + 5_000_000_000L < System.nanoTime()) {
          double seconds = totalTime / 1_000_000_000.0;
          double byteRate = bytesSent / 1024 / 1024 / seconds;
          double latency = seconds / count;
          System.out.printf("%.2f MB/s @ avg lat %.4f ms%n", byteRate, latency);
          client.releaseJournals(volumeId, blockId, generation);
          start = System.nanoTime();
          totalTime = 0;
          bytesSent = 0;
          count = 0;
        }
        generation++;
        random.nextBytes(buffer);
        long position = random.nextInt(Integer.MAX_VALUE);
        bytesSent += buffer.length;
        long s = System.nanoTime();
        client.write(volumeId, blockId, generation, position, buffer);
        long e = System.nanoTime();
        totalTime += (e - s);
        count++;
      }
    }
  }

}
