package pack.iscsi.s3.volume;

import java.net.InetAddress;
import java.net.UnknownHostException;

import consistent.s3.ConsistentAmazonS3;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3VolumeStoreConfig {

  static final String HOSTNAME;

  static {
    String hostname;
    try {
      hostname = InetAddress.getLocalHost()
                            .getHostName();
    } catch (UnknownHostException e) {
      hostname = null;
    }
    HOSTNAME = hostname;
  }

  ConsistentAmazonS3 consistentAmazonS3;

  String bucket;

  String objectPrefix;

  @Builder.Default
  String hostname = HOSTNAME;
}
