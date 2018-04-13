package pack.distributed.storage.hdfs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.kvs.BytesRef;
import pack.distributed.storage.hdfs.kvs.rpc.BytesReference;
import pack.distributed.storage.hdfs.kvs.rpc.Pair;
import pack.distributed.storage.hdfs.kvs.rpc.RemoteKeyValueStoreClient;
import pack.distributed.storage.hdfs.kvs.rpc.RemoteKeyValueStoreServer;
import pack.distributed.storage.hdfs.kvs.rpc.ScanResult;
import pack.distributed.storage.hdfs.kvs.rpc.TransId;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.wal.Block;
import pack.distributed.storage.wal.Blocks;
import pack.distributed.storage.wal.PackWalFactory;
import pack.distributed.storage.wal.PackWalReader;
import pack.distributed.storage.wal.PackWalWriter;
import pack.distributed.storage.walcache.WalCacheManager;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.storage.utils.PackUtils;

public class PackHdfsWalFactory extends PackWalFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackHdfsWalFactory.class);

  protected static final int LONG_TYPE_SIZE = 8;

  private final RemoteKeyValueStoreClient _client;
  private final ZooKeeperClient _zk;
  private final RemoteKeyValueStoreServer _server;

  public PackHdfsWalFactory(Configuration configuration, String zkConnection, int sessionTimeout,
      String hdfsWalBindAddress, int hdfsWalPort, Path hdfsWalDir, UserGroupInformation ugi) throws IOException {
    _zk = ZkUtils.newZooKeeper(zkConnection, sessionTimeout);
    _server = RemoteKeyValueStoreServer.createInstance(hdfsWalBindAddress, hdfsWalPort, configuration, _zk, hdfsWalDir,
        ugi);
    _server.start();
    _client = RemoteKeyValueStoreClient.create(configuration, _zk);
  }

  @Override
  public PackWalWriter createPackWalWriter(String name, PackMetaData metaData, WriteBlockMonitor writeBlockMonitor,
      BroadcastServerManager serverStatusManager) throws IOException {
    return new PackWalWriter(name, writeBlockMonitor, serverStatusManager) {

      private TransId _transId;

      @Override
      protected synchronized void writeBlocks(Blocks blocks) throws IOException {
        byte[] bytes = Blocks.toBytes(blocks);
        _transId = _client.putIncrement(name, BytesReference.toBytesReference(new BytesRef(LONG_TYPE_SIZE)),
            BytesReference.toBytesReference(new BytesRef(bytes)));
      }

      @Override
      protected void internalFlush() throws IOException {
        _client.sync(name, _transId);
      }

      @Override
      protected void internalClose() throws IOException {

      }
    };
  }

  @Override
  public PackWalReader createPackWalReader(String name, PackMetaData metaData, WalCacheManager walCacheManager,
      MaxBlockLayer maxBlockLayer) throws IOException {
    return new PackWalReader(name, walCacheManager) {

      private final AtomicLong _lastOffset = new AtomicLong(-1L);

      @Override
      protected void writeDataToWal(WalCacheManager walCacheManager) throws IOException {
        while (isRunning()) {
          cleanupOldData();
          ScanResult scanResult = _client.scan(name, getScanStart());
          boolean empty = true;
          List<Pair<BytesRef, BytesRef>> result = scanResult.getResult();
          for (Pair<BytesRef, BytesRef> pair : result) {
            empty = false;
            BytesRef key = pair.getT1();
            BytesRef value = pair.getT2();
            long offset = PackUtils.getLong(key.bytes, key.offset);
            Blocks blocks = Blocks.toBlocks(getBytes(value));
            // LOGGER.info("read blocks {} {} {}", Md5Utils.md5AsBase64(bs),
            // blocks.hashCode(), _zkPath + "/" + s);
            List<Block> blocksList = blocks.getBlocks();
            _lastOffset.set(offset);
            for (Block block : blocksList) {
              int blockId = block.getBlockId();
              long transId = block.getTransId();
              byte[] data = block.getData();
              // LOGGER.info("wal write {} {} {}", transId, offset,
              // blockId);
              walCacheManager.write(transId, offset, blockId, data);
            }
          }
          if (empty) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }

      private BytesReference getScanStart() {
        long l = _lastOffset.get();
        return BytesReference.toBytesReference(new BytesRef(l + 1));
      }

      private void cleanupOldData() throws IOException {
        long maxLayer = maxBlockLayer.getMaxLayer();
        if (maxLayer == 0L) {
          return;
        }
        _client.deleteRange(name, BytesReference.toBytesReference(BytesRef.value(0L)),
            BytesReference.toBytesReference(BytesRef.value(maxLayer)));
      }

      @Override
      public void sync() throws IOException {
        long sleep = 1;
        while (true) {
          BytesReference lastKeyBytesReference = _client.lastKey(name);
          BytesRef lastKey = lastKeyBytesReference.getBytesRef();
          if (lastKey != null) {
            long offset = PackUtils.getLong(lastKey.bytes, lastKey.offset);
            LOGGER.info("lastKey {} offset {}", lastKey, offset);
            if (offset == _lastOffset.get()) {
              return;
            }
          } else {
            LOGGER.info("lastKey {}", lastKey);
          }
          if (sleep < 1000) {
            sleep++;
          }
          try {
            Thread.sleep(sleep);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      }

      private byte[] getBytes(BytesRef value) {
        if (value.offset == 0 && value.length == value.bytes.length) {
          return value.bytes;
        }
        byte[] bs = new byte[value.length];
        System.arraycopy(value.bytes, value.offset, bs, 0, bs.length);
        return bs;
      }
    };
  }

  @Override
  public void close() throws IOException {
    PackUtils.close(LOGGER, _server, _zk);
  }

}
