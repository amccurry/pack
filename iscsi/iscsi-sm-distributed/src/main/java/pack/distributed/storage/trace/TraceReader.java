package pack.distributed.storage.trace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.Md5Utils;

import pack.distributed.storage.BlockReader;
import pack.distributed.storage.hdfs.BlockFile.BlockFileEntry;
import pack.distributed.storage.hdfs.BlockFile.Reader;
import pack.distributed.storage.hdfs.ReadRequest;

public class TraceReader extends Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceReader.class);

  private final Reader _reader;
  private final Map<Integer, String> _hashMap;

  public TraceReader(Map<Integer, String> hashMap, Reader reader) {
    _reader = reader;
    _hashMap = hashMap;
  }

  public boolean readBlocks(List<ReadRequest> requests) throws IOException {
    return trace(LOGGER, _reader, _hashMap, requests);
  }

  public void close() throws IOException {
    _reader.close();
  }

  public Iterator<BlockFileEntry> iterator() {
    return _reader.iterator();
  }

  public List<BlockReader> getLeaves() {
    return _reader.getLeaves();
  }

  public boolean readBlocks(ReadRequest... requests) throws IOException {
    return _reader.readBlocks(requests);
  }

  public boolean read(Collection<ReadRequest> requests) throws IOException {
    return _reader.read(requests);
  }

  public boolean read(int key, BytesWritable value) throws IOException {
    return _reader.read(key, value);
  }

  public void orDataBlocks(RoaringBitmap bitmap) {
    _reader.orDataBlocks(bitmap);
  }

  public void orEmptyBlocks(RoaringBitmap bitmap) {
    _reader.orEmptyBlocks(bitmap);
  }

  public boolean hasEmptyBlock(int blockId) {
    return _reader.hasEmptyBlock(blockId);
  }

  public boolean hasBlock(int blockId) {
    return _reader.hasBlock(blockId);
  }

  public Path getPath() {
    return _reader.getPath();
  }

  public int getBlockSize() {
    return _reader.getBlockSize();
  }

  public List<String> getSourceBlockFiles() {
    return _reader.getSourceBlockFiles();
  }

  public static boolean trace(Logger logger, BlockReader blockReader, Map<Integer, String> hashes,
      List<ReadRequest> requests) throws IOException {
    boolean[] completedBefore = new boolean[requests.size()];
    for (int i = 0; i < requests.size(); i++) {
      ReadRequest readRequest = requests.get(i);
      completedBefore[i] = readRequest.isCompleted();
    }
    boolean readBlocks = blockReader.readBlocks(requests);
    for (int i = 0; i < requests.size(); i++) {
      if (!completedBefore[i]) {
        ReadRequest readRequest = requests.get(i);
        int blockId = readRequest.getBlockId();
        String current = hashes.get(blockId);
        logger.debug("Read {} {}", blockId, current);
        if (readRequest.isCompleted()) {
          // Complete during this readBlocks call
          if (current != null) {
            ByteBuffer duplicate = readRequest.getByteBuffer()
                                              .duplicate();
            duplicate.flip();
            byte[] value = new byte[duplicate.remaining()];
            duplicate.get(value);
            String bs = Md5Utils.md5AsBase64(value);
            if (!current.equals(bs)) {
              logger.error("Block id {} not equal {} {} {}", blockId, current, bs, value.length);
              for (ReadRequest request : requests) {

                logger.info("read request {} {} {}", request.isCompleted(), request.getBlockId(),
                    readRequest.getBlockOffset());

                byte[] bb = new byte[value.length];
                ReadRequest rr = new ReadRequest(blockId, 0, ByteBuffer.wrap(bb));
                boolean completed = blockReader.readBlocks(rr);
                if (!completed) {
                  logger.info("second attempt {}", Md5Utils.md5AsBase64(bb));
                } else {
                  logger.error("second attempt miss", Md5Utils.md5AsBase64(bb));
                }
              }
            }
          } else {
            logger.error("HIT when shouldn't {}", blockId);
          }
        } else {
          // Not completed during this readBlocks call
          if (current != null) {
            logger.error("MISS when shouldn't {} {}", blockId, current);
          }
        }
      }
    }
    return readBlocks;
  }
}
