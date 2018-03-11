package org.jscsi.target;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.DigestException;
import java.util.Collection;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.ParserConfigurationException;

import org.jscsi.exception.InternetSCSIException;
import org.jscsi.parser.OperationCode;
import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.parser.login.ISID;
import org.jscsi.parser.login.LoginRequestParser;
import org.jscsi.target.connection.Connection;
import org.jscsi.target.connection.Connection.TargetConnection;
import org.jscsi.target.connection.TargetSession;
import org.jscsi.target.scsi.inquiry.DeviceIdentificationVpdPage;
import org.jscsi.target.scsi.inquiry.UnitSerialNumberVpdPage;
import org.jscsi.target.settings.SettingsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import pack.iscsi.storage.StorageTargetManager;
import pack.iscsi.storage.utils.PackUtils;

/**
 * The central class of the jSCSI Target, which keeps track of all active
 * {@link TargetSession}s, stores target-wide parameters and variables, and
 * which contains the {@link #main(String[])} method for starting the program.
 * 
 * @author Andreas Ergenzinger, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 */
public final class TargetServer implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TargetServer.class);

  /**
   * A {@link SocketChannel} used for listening to incoming connections.
   */
  private ServerSocketChannel serverSocketChannel;

  /**
   * Contains all active {@link TargetSession}s.
   */
  private Collection<TargetSession> sessions = new Vector<TargetSession>();

  /**
   * The jSCSI Target's global parameters.
   */
  private Configuration config;

  /**
   * 
   */
  private DeviceIdentificationVpdPage deviceIdentificationVpdPage;

  /**
   * A target-wide counter used for providing the value of sent
   * {@link ProtocolDataUnit}s' <code>Target Transfer Tag</code> field, unless
   * that field is reserved.
   */
  private static final AtomicInteger nextTargetTransferTag = new AtomicInteger();

  /**
   * The connection the target server is using.
   */
  private Connection connection;

  /**
   * while this value is true, the target is active.
   */
  private boolean running = true;

  private ExecutorService threadPool;

  private StorageTargetManager iscsiTargetManager;

  private InetAddress targetAddress;

  private int targetPort;

  private UnitSerialNumberVpdPage unitSerialNumber = new UnitSerialNumberVpdPage(UUID.randomUUID());

  private InetAddress bindAddress;

  private int bindPort;

  public TargetServer(InetAddress targetAddress, int targetPort, StorageTargetManager iscsiTargetManager,
      InetAddress bindAddress, int bindPort) throws IOException {
    this.bindAddress = bindAddress;
    this.bindPort = bindPort;
    this.targetAddress = targetAddress;
    this.targetPort = targetPort;
    try {
      config = Configuration.create(extractFile("/jscsi-target.xsd"), extractFile("/jscsi-target.xml"),
          targetAddress.getHostAddress());
    } catch (SAXException | ParserConfigurationException | IOException e) {
      throw new IOException(e);
    }
    config.port = targetPort;
    this.iscsiTargetManager = iscsiTargetManager;
    threadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new TargetThreadFactory());
    LOGGER.debug("Starting jSCSI-target: ");
    LOGGER.debug("   addr:           " + targetAddress);
    LOGGER.debug("   port:           " + targetPort);
    this.deviceIdentificationVpdPage = new DeviceIdentificationVpdPage(this);
  }

  private File extractFile(String resource) throws IOException {
    File file = new File("/tmp/.jscsi" + resource);
    file.getParentFile()
        .mkdirs();
    try (InputStream input = getClass().getResourceAsStream(resource)) {
      try (FileOutputStream output = new FileOutputStream(file)) {
        PackUtils.copy(input, output);
        return file;
      }
    }
  }

  static class TargetThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    TargetThreadFactory() {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
                                                       .getThreadGroup();
      namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      t.setUncaughtExceptionHandler((t1, e) -> LOGGER.error("Unknown error", e));
      return t;
    }
  }

  public InetAddress getTargetAddress() {
    return targetAddress;
  }

  public void setTargetAddress(InetAddress targetAddress) {
    this.targetAddress = targetAddress;
  }

  public int getTargetPort() {
    return targetPort;
  }

  public void setTargetPort(int targetPort) {
    this.targetPort = targetPort;
  }

  /**
   * Gets and increments the value to use in the next unreserved
   * <code>Target Transfer Tag</code> field of the next PDU to be sent by the
   * jSCSI Target.
   * 
   * @see #nextTargetTransferTag
   * @return the value to use in the next unreserved <code>Target Transfer Tag
   * </code> field
   */
  public static int getNextTargetTransferTag() {
    // value 0xffffffff is reserved
    int tag;
    do {
      tag = nextTargetTransferTag.getAndIncrement();
    } while (tag == -1);
    return tag;
  }

  public Void call() throws Exception {
    LOGGER.info("Starting target server {}", targetAddress);
    // Create a blocking server socket and check for connections
    try {
      // Create a blocking server socket channel on the specified/default
      // port
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(true);

      // Making sure the socket is bound to the address used in the config.
      serverSocketChannel.socket()
                         .bind(new InetSocketAddress(bindAddress, bindPort));

      while (running) {
        // Accept the connection request.
        // If serverSocketChannel is blocking, this method blocks.
        // The returned channel is in blocking mode.
        final SocketChannel socketChannel = serverSocketChannel.accept();

        // deactivate Nagle algorithm
        socketChannel.socket()
                     .setTcpNoDelay(true);

        connection = new TargetConnection(socketChannel, true);
        try {
          final ProtocolDataUnit pdu = connection.receivePdu();
          // confirm OpCode-
          if (pdu.getBasicHeaderSegment()
                 .getOpCode() != OperationCode.LOGIN_REQUEST)
            throw new InternetSCSIException();
          // get initiatorSessionID

          LoginRequestParser parser = (LoginRequestParser) pdu.getBasicHeaderSegment()
                                                              .getParser();
          ISID initiatorSessionID = parser.getInitiatorSessionID();

          /*
           * TODO get (new or existing) session based on TSIH But since we don't
           * do session reinstatement and MaxConnections=1, we can just create a
           * new one.
           */
          // setExpCmdSN (PDU is immediate, hence no ++)
          TargetSession session = new TargetSession(this, connection, initiatorSessionID,
              parser.getCommandSequenceNumber(), parser.getExpectedStatusSequenceNumber());
          sessions.add(session);
          threadPool.submit(() -> {
            try {
              return connection.call();
            } catch (Throwable e) {
              LOGGER.error("Unknown error", e);
              throw e;
            }
          });
        } catch (DigestException | InternetSCSIException | SettingsException | IOException e) {
          LOGGER.info("Throws Exception", e);
          continue;
        }
      }
    } catch (IOException e) {
      // this block is entered if the desired port is already in use
      LOGGER.error("Throws Exception", e);
    }

    LOGGER.info("Closing socket channel.");
    serverSocketChannel.close();
    for (TargetSession session : sessions) {
      session.getStorageModule()
             .close();
    }
    return null;
  }

  public Collection<TargetSession> getSessions() {
    return sessions;
  }

  public Configuration getConfig() {
    return config;
  }

  public DeviceIdentificationVpdPage getDeviceIdentificationVpdPage() {
    return deviceIdentificationVpdPage;
  }

  public Target getTarget(String targetName) {
    if (targetName == null) {
      return null;
    }
    return iscsiTargetManager.getTarget(targetName);
  }

  /**
   * Removes a session from the jSCSI Target's list of active sessions.
   * 
   * @param session
   *          the session to remove from the list of active sessions
   */
  public synchronized void removeTargetSession(TargetSession session) {
    sessions.remove(session);
  }

  public String[] getTargetNames() {
    return iscsiTargetManager.getTargetNames();
  }

  /**
   * Checks to see if this target name is valid.
   * 
   * @param checkTargetName
   * @return true if the the target name is configured
   */
  public boolean isValidTargetName(String checkTargetName) {
    return iscsiTargetManager.isValidTarget(checkTargetName);
  }

  /**
   * Using this connection mainly for test pruposes.
   * 
   * @return the connection the target server established.
   */
  public Connection getConnection() {
    return this.connection;
  }

  /**
   * Stop this target server
   */
  public void stop() {
    this.running = false;
    for (TargetSession session : sessions) {
      if (!session.getConnection()
                  .stop()) {
        this.running = true;
        LOGGER.error("Unable to stop session for " + session.getTargetName());
      }
    }
  }

  public UnitSerialNumberVpdPage getUnitSerialNumber() {
    return unitSerialNumber;
  }

}
