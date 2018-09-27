package pack.block.blockstore.crc;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrcLayerImpl implements Closeable, CrcLayer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrcLayerImpl.class);

  public static void main(String[] args) throws IOException {

    try (CrcLayerImpl layer = new CrcLayerImpl("test", 100, 4096)) {
      Random random = new Random(1);
      byte[] buf = new byte[128];
      for (int i = 0; i < 100; i++) {
        random.nextBytes(buf);
        layer.put(i, buf);
      }
      // random = new Random(1);
      for (int i = 0; i < 100; i++) {
        random.nextBytes(buf);
        layer.validate("", i, buf);
      }
    }
  }

  private final ByteBuffer _crcs;
  private final BitSet _presentEntries;
  private final RandomAccessFile _randomAccessFile;
  private final FileChannel _channel;
  private final String _name;
  private final int _blockSize;
  private final int _emptyCrc;

  public CrcLayerImpl(String name, int entries, int blockSize) throws IOException {
    _blockSize = blockSize;
    _name = name;
    _presentEntries = new BitSet(entries);
    File file = new File(UUID.randomUUID()
                             .toString()
        + ".crc");
    file.deleteOnExit();
    _randomAccessFile = new RandomAccessFile(file, "rw");
    _randomAccessFile.setLength(entries * 4);
    _channel = _randomAccessFile.getChannel();
    _crcs = _channel.map(MapMode.READ_WRITE, 0, entries * 4);
    CRC32 crc32 = new CRC32();
    crc32.update(new byte[blockSize]);
    _emptyCrc = (int) crc32.getValue();
  }

  @Override
  public int getBlockSize() {
    return _blockSize;
  }

  @Override
  public void close() throws IOException {
    _channel.close();
    _randomAccessFile.close();
  }

  @Override
  public synchronized void put(int blockId, byte[] buf) {
    CRC32 crc32 = new CRC32();
    crc32.update(buf);
    _crcs.putInt(blockId * 4, (int) crc32.getValue());
    _presentEntries.set(blockId);
  }

  @Override
  public synchronized void validate(Object msg, int blockId, byte[] buf) {
    validate(msg, blockId, buf, 0, buf.length);
  }

  @Override
  public synchronized void put(int blockId, ByteBuffer byteBuffer) {
    ByteBuffer duplicate = byteBuffer.duplicate();
    byte[] bs = new byte[duplicate.remaining()];
    duplicate.get(bs);
    put(blockId, bs);
  }

  @Override
  public synchronized void validateDeleted(Object msg, int blockId) {
    if (_presentEntries.get(blockId)) {
      int validCrc = _crcs.getInt(blockId * 4);
      if (validCrc != _emptyCrc) {
        LOGGER.error("CRC {} error on block {} expected {} actual {} msg {}", _name, blockId, validCrc, _emptyCrc, msg);
      }
    }
  }

  @Override
  public synchronized void delete(int startingBlockId, int endingBlockId) {
    for (; startingBlockId < endingBlockId; startingBlockId++) {
      _crcs.putInt(startingBlockId * 4, _emptyCrc);
      _presentEntries.set(startingBlockId);
    }
  }

  @Override
  public synchronized void validate(Object msg, int blockId, byte[] buf, int off, int len) {
    if (_presentEntries.get(blockId)) {
      CRC32 crc32 = new CRC32();
      try {
        crc32.update(buf, off, len);
      } catch (ArrayIndexOutOfBoundsException e) {
        LOGGER.error("ArrayIndexOutOfBoundsException buf {} off {} len {}", buf.length, off, len);
      }
      int validCrc = _crcs.getInt(blockId * 4);
      int dataCrc = (int) crc32.getValue();
      if (validCrc != dataCrc) {
        LOGGER.error("CRC {} error on block {} expected {} actual {} msg {}", _name, blockId, validCrc, dataCrc, msg);
      }
    }
  }

}
