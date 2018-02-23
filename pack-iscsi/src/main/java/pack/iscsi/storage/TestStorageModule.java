package pack.iscsi.storage;

import java.io.IOException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import pack.block.blockstore.hdfs.blockstore.LocalWalCache;
import pack.iscsi.BaseIStorageModule;

public class TestStorageModule extends BaseIStorageModule {

  public static void main(String[] args) {

  }

  private final TestStorageModuleConfig _config;

  public TestStorageModule(TestStorageModuleConfig config) {
    super(config.getSizeInBytes());
    _config = config;
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    // check that we are up to date on stream
    // not sure yet?
    // read local wal
    LocalWalCache localWalCache = getLocalWalCache();
    // read remote blocks
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    // write to topic
    _config.getProducer()
           .send(getRecord(bytes, storageIndex), new Callback() {
             
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
              metadata.
            }
          });
    
  }

  private ProducerRecord<byte[], byte[]> getRecord(byte[] bytes, long storageIndex) {
    return new ProducerRecord<byte[], byte[]>(_config.getTopic(), getKey(storageIndex), bytes);
  }

  private byte[] getKey(long storageIndex) {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

}
