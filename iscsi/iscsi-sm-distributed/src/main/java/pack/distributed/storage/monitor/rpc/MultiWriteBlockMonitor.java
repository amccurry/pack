package pack.distributed.storage.monitor.rpc;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.google.common.io.Closer;

public interface MultiWriteBlockMonitor extends Closeable {

  public static final String UTF_8 = "UTF-8";

  int registerVolume(String volumeName) throws IOException;

  int registerServer(String server) throws IOException;

  void addDirtyBlock(int volumeId, int blockId, long transId) throws IOException;

  void resetDirtyBlock(int serverId, int volumeId, int blockId, long transId) throws IOException;

  void waitIfNeededForSync(int serverId, int volumeId, int blockId) throws IOException;

  public static MultiWriteBlockMonitor createClientTcp(String host, int port) throws IOException {
    Closer closer = Closer.create();
    try {
      Socket socket = closer.register(new Socket(host, port));
      socket.setTcpNoDelay(true);
      socket.setKeepAlive(true);
      DataInputStream inputStream = closer.register(new DataInputStream(socket.getInputStream()));
      DataOutputStream outputStream = closer.register(new DataOutputStream(socket.getOutputStream()));
      return new MultiWriteBlockMonitor() {

        @Override
        public void waitIfNeededForSync(int serverId, int volumeId, int blockId) throws IOException {
          outputStream.write(2);
          outputStream.writeInt(serverId);
          outputStream.writeInt(volumeId);
          outputStream.writeInt(blockId);
          outputStream.flush();
          if (inputStream.read() != 0) {
            throw new IOException("Unknown error");
          }
        }

        @Override
        public void resetDirtyBlock(int serverId, int volumeId, int blockId, long transId) throws IOException {
          outputStream.write(1);
          outputStream.writeInt(serverId);
          outputStream.writeInt(volumeId);
          outputStream.writeInt(blockId);
          outputStream.writeLong(transId);
          outputStream.flush();
          if (inputStream.read() != 0) {
            throw new IOException("Unknown error");
          }
        }

        @Override
        public void addDirtyBlock(int volumeId, int blockId, long transId) throws IOException {
          outputStream.write(0);
          outputStream.writeInt(volumeId);
          outputStream.writeInt(blockId);
          outputStream.writeLong(transId);
          outputStream.flush();
          if (inputStream.read() != 0) {
            throw new IOException("Unknown error");
          }
        }
        
        @Override
        public int registerServer(String serverName) throws IOException {
          outputStream.write(4);
          writeString(outputStream, serverName);
          outputStream.flush();
          if (inputStream.read() != 0) {
            throw new IOException("Unknown error");
          }
          return inputStream.readInt();
        }

        @Override
        public int registerVolume(String volumeName) throws IOException {
          outputStream.write(3);
          writeString(outputStream, volumeName);
          outputStream.flush();
          if (inputStream.read() != 0) {
            throw new IOException("Unknown error");
          }
          return inputStream.readInt();
        }

        private void writeString(DataOutputStream outputStream, String s) throws IOException {
          byte[] bs = s.getBytes(UTF_8);
          outputStream.writeInt(bs.length);
          outputStream.write(bs);
        }

        @Override
        public void close() throws IOException {
          closer.close();
        }
      };
    } catch (IOException e) {
      closer.close();
      throw e;
    }
  }

}
