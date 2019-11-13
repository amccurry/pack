package pack.iscsi.brick.remote.server;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BrickServerConfig {

  List<File> brickDirs;

  @Builder.Default
  String address = "0.0.0.0";

  @Builder.Default
  int port = 0;

  CuratorFramework curatorFramework;

  String zkPrefix;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(1000);

  @Builder.Default
  int minThreads = 10;

  @Builder.Default
  int maxThreads = 10;

  @Builder.Default
  boolean nonBlockingRpc = false;

  @Builder.Default
  boolean directIO = false;

  @Builder.Default
  boolean async = false;

  @Builder.Default
  boolean kerberosEnabled = false;

  @Builder.Default
  String kerberosProtocol = "pack";

  String kerberosServerName;

  @Builder.Default
  boolean sslEnabled = false;

  @Builder.Default
  boolean sslClientAuthEnabled = false;

  @Builder.Default
  String sslProtocol = "TLS";

  String[] sslCipherSuites;

  String sslKeyStore;

  String sslKeyPass;

  String sslTrustStore;

  String sslTrustPass;

}
