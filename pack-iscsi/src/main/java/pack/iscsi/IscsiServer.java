package pack.iscsi;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jscsi.target.TargetServer;
import org.jscsi.target.storage.IStorageModule;
import org.jscsi.target.storage.SynchronizedRandomAccessStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class IscsiServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServer.class);

  private final TargetServer _targetServer;
  private final ExecutorService _executorService;
  private Future<Void> _future;

  public IscsiServer(String address, int port, TargetManager iscsiTargetManager) throws IOException {
    _targetServer = new TargetServer(InetAddress.getByName(address), port, iscsiTargetManager);
    _executorService = Executors.newSingleThreadExecutor();
  }

  public void start() {
    _future = _executorService.submit(_targetServer);
  }

  public void join() throws InterruptedException, ExecutionException {
    _future.get();
  }

  @Override
  public void close() throws IOException {
    _future.cancel(true);
    _executorService.shutdownNow();
  }

  public static void main(String[] args) throws Exception {

    String address = "192.168.56.1";
    String date = "2018-02";
    String domain = "com.fortitudetec";

    TargetManager iscsiTargetManager = new BaseTargetManager(date, domain);

    // MetricRegistry registry = new MetricRegistry();
    // Configuration configuration = new Configuration();
    // Path root = new Path("/pack");
    // String name = "testvolume1";
    // Path volumePath = new Path(root, name);
    // UserGroupInformation ugi = UserGroupInformation.createRemoteUser("pack");
    // ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
    // FileSystem fileSystem = root.getFileSystem(configuration);
    // if (!fileSystem.exists(volumePath)) {
    // HdfsBlockStoreAdmin.writeHdfsMetaData(HdfsMetaData.DEFAULT_META_DATA,
    // fileSystem, volumePath);
    // }
    // return null;
    // });
    //
    // File cacheDir = new File("./cache");
    // HdfsBlockStore blockStore =
    // ugi.doAs((PrivilegedExceptionAction<HdfsBlockStore>) () -> {
    // FileSystem fileSystem = root.getFileSystem(configuration);
    // return new HdfsBlockStoreImpl(registry, cacheDir, fileSystem,
    // volumePath);
    // });
    //
    // UgiHdfsBlockStore store = UgiHdfsBlockStore.wrap(ugi, blockStore);
    // IStorageModule module = new HdfsStorageModule(store);

    File file = new File("./storage/test1");
    IStorageModule module = getStorageModule(file);
    String name = "test1";
    iscsiTargetManager.register(name, "pack - " + name, module);
    try (IscsiServer iscsiServer1 = new IscsiServer(address, 3260, iscsiTargetManager)) {
      try (IscsiServer iscsiServer2 = new IscsiServer("192.168.1.192", 3260, iscsiTargetManager)) {
        iscsiServer1.start();
        iscsiServer2.start();
        iscsiServer1.join();
        iscsiServer2.join();
      }
    }
  }

  private static IStorageModule getStorageModule(File file) throws FileNotFoundException {
    return new SynchronizedRandomAccessStorageModule((file.length() / IStorageModule.VIRTUAL_BLOCK_SIZE), file) {

      @Override
      public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
          Integer commandSequenceNumber) throws IOException {
        // BasicHeaderSegment basicHeaderSegment = pdu.getBasicHeaderSegment();
        // int initiatorTaskTag = basicHeaderSegment.getInitiatorTaskTag();
        // AbstractMessageParser parser = pdu.getBasicHeaderSegment()
        // .getParser();
        // if (parser instanceof InitiatorMessageParser) {
        // InitiatorMessageParser initiatorMessageParser =
        // (InitiatorMessageParser) parser;
        // int commandSequenceNumber =
        // initiatorMessageParser.getCommandSequenceNumber();
        // LOGGER.info("{} {} read initTag {} comSeqNum {} index {} length {} ",
        // address, port, initiatorTaskTag,
        // commandSequenceNumber, storageIndex, bytes.length);
        // super.read(pdu, bytes, storageIndex);
        // } else {
        // LOGGER.info("read other.....");
        // super.read(pdu, bytes, storageIndex);
        // }
        LOGGER.info("{} {} read initTag {} comSeqNum {} index {} length {} ", address, port, initiatorTaskTag,
            commandSequenceNumber, storageIndex, bytes.length);
        super.read(bytes, storageIndex);
      }

      @Override
      public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
          Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
        // BasicHeaderSegment basicHeaderSegment = pdu.getBasicHeaderSegment();
        // int initiatorTaskTag = basicHeaderSegment.getInitiatorTaskTag();
        // AbstractMessageParser parser = pdu.getBasicHeaderSegment()
        // .getParser();
        // if (parser instanceof InitiatorMessageParser) {
        // int dataSequenceNumber = -1;
        // int targetTransferTag = -1;
        // InitiatorMessageParser initiatorMessageParser =
        // (InitiatorMessageParser) parser;
        // int commandSequenceNumber =
        // initiatorMessageParser.getCommandSequenceNumber();
        // if (initiatorMessageParser instanceof DataOutParser) {
        // DataOutParser dataOutParser = (DataOutParser) initiatorMessageParser;
        // dataSequenceNumber = dataOutParser.getDataSequenceNumber();
        // targetTransferTag = dataOutParser.getTargetTransferTag();
        // }
        // LOGGER.info("{} {} write initTag {} comSeqNum {} TargetTransTag {}
        // DataDeqNum {} index {} length {}", address,
        // port, initiatorTaskTag, commandSequenceNumber, targetTransferTag,
        // dataSequenceNumber, storageIndex,
        // bytes.length);
        // super.write(pdu, bytes, storageIndex);
        // } else {
        // LOGGER.info("write other.....");
        // super.write(pdu, bytes, storageIndex);
        // }

        LOGGER.info("{} {} write initTag {} comSeqNum {} TargetTransTag {} DataDeqNum {} index {} length {}", address,
            port, initiatorTaskTag, commandSequenceNumber, targetTransferTag, dataSequenceNumber, storageIndex,
            bytes.length);
        super.write(bytes, storageIndex);
      }
    };
  }

}
