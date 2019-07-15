package pack.block.blockserver;

import java.io.File;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hadoop.rpc.HadoopRpcServerFactory;
import pack.block.blockserver.local.LocalBlockServer;
import pack.block.util.Utils;

public class BlockServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockServerMain.class);

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    UserGroupInformation.setConfiguration(configuration);
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    
    BlockServer instance = new LocalBlockServer(new File("./bs-store"));
    
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      LOGGER.info("Creating server with ugi {}", UserGroupInformation.getCurrentUser());
      HadoopRpcServerFactory<BlockServer> factory = new HadoopRpcServerFactory<>(configuration, BlockServer.class);
      Server server = factory.createServer("0.0.0.0", 9000, 10, instance);
      server.start();
      server.join();
      return null;
    });
  }

}
