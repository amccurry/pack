package pack.iscsi.storage;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.BaseIStorageModule;
import pack.iscsi.storage.utils.IOUtils;

public class PackStorageModule extends BaseIStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackStorageModule.class);

  private final StorageManager _manager;

  public PackStorageModule(long sizeInBytes, StorageManager manager) {
    super(sizeInBytes);
    _manager = manager;
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    LOGGER.info("read {} {}", storageIndex, bytes.length);
    try {
      if (bytes.length == 0) {
        return;
      }
      _manager.waitForKafkaSyncIfNeeded();
      List<ReadRequest> requests = _manager.createReadRequest(bytes, storageIndex);
      BlockReader blockReader = _manager.getBlockReader();
      blockReader.readBlocks(requests);
    } catch (IOException e) {
      LOGGER.error("unknown error", e);
      if (e instanceof IOException) {
        throw e;
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    LOGGER.info("write {} {}", storageIndex, bytes.length);
    try {
      assertIsValidForWriting(storageIndex, bytes.length);
      KafkaProducer<Long, byte[]> producer = _manager.getKafkaProducer();
      producer.send(getRecord(bytes, storageIndex));
    } catch (IOException e) {
      LOGGER.error("unknown error", e);
      if (e instanceof IOException) {
        throw e;
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void flushWrites() throws IOException {
    LOGGER.info("flushWrites");
    try {
      KafkaProducer<Long, byte[]> producer = _manager.getKafkaProducer();
      producer.flush();
    } catch (Throwable e) {
      LOGGER.error("unknown error", e);
      if (e instanceof IOException) {
        throw e;
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    flushWrites();
    IOUtils.closeQuietly(_manager);
  }

  private void assertIsValidForWriting(long storageIndex, int length) throws IOException {
    _manager.assertIsValidForWriting(storageIndex, length);
  }

  private ProducerRecord<Long, byte[]> getRecord(byte[] bytes, long storageIndex) {
    return new ProducerRecord<Long, byte[]>(_manager.getKafkaTopic(), _manager.getKafkaPartition(), storageIndex,
        bytes);
  }
}
