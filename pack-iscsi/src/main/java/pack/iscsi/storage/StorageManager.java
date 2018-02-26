package pack.iscsi.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;

import pack.iscsi.storage.utils.IOUtils;

public class StorageManager implements Closeable {

  private final PackStorageMetaData _metaData;
  private final String _topic;
  private final Integer _partition;
  private final int _blockSize;
  private final DataSyncManager _dataSyncManager;
  private final DataArchiveManager _dataArchiveManager;

  public StorageManager(PackStorageMetaData metaData, DataSyncManager dataSyncManager,
      DataArchiveManager dataArchiveManager) {
    _metaData = metaData;
    _topic = _metaData.getKafkaTopic();
    _partition = _metaData.getKafkaPartition();
    _blockSize = _metaData.getBlockSize();
    _dataSyncManager = dataSyncManager;
    _dataArchiveManager = dataArchiveManager;
  }

  public void waitForKafkaSyncIfNeeded() throws IOException {
    try {
      _dataSyncManager.waitForKafkaSyncIfNeeded();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public BlockReader getBlockReader() throws IOException {
    BlockReader syncReader = _dataSyncManager.getBlockReader();
    BlockReader archiveReader = _dataArchiveManager.getBlockReader();
    return BlockReader.mergeInOrder(syncReader, archiveReader);
  }

  public List<ReadRequest> createReadRequest(byte[] bytes, long storageIndex) {
    return IOUtils.createRequests(ByteBuffer.wrap(bytes), storageIndex, _blockSize);
  }

  public KafkaProducer<Long, byte[]> getKafkaProducer() {
    return _dataSyncManager.getKafkaProducer();
  }

  public String getKafkaTopic() {
    return _topic;
  }

  public Integer getKafkaPartition() {
    return _partition;
  }

  public void assertIsValidForWriting(long storageIndex, int length) throws IOException {
    IOUtils.assertIsValidForWriting(storageIndex, length, _blockSize);
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_dataSyncManager);
    IOUtils.closeQuietly(_dataArchiveManager);
  }

}
