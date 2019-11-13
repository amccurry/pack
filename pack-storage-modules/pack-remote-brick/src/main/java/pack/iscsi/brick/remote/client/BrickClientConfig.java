package pack.iscsi.brick.remote.client;

import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BrickClientConfig {

  String hostname;

  @Builder.Default
  int port = 8312;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(1000);

  @Builder.Default
  int maxClientCount = 10;

  @Builder.Default
  boolean kerberosEnabled = false;

  @Builder.Default
  String kerberosProtocol = "pack";

  String kerberosServerName;

  @Builder.Default
  boolean sslEnabled = false;

  @Builder.Default
  boolean sslClientAuthEnabled = false;

  String sslKeyStore;

  String sslKeyPass;

  String sslTrustStore;

  String sslTrustPass;

}
