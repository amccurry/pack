package pack.block.s3;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;

import pack.s3.MetadataStore;

public class S3MetadataStore implements MetadataStore {

  private static final Joiner KEY_JOINER = Joiner.on('/');

  private final String _bucketName;
  private final String _prefix;
  private final AmazonS3 _client;

  public static void main(String[] args) throws Exception {
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    S3MetadataStoreConfig config = S3MetadataStoreConfig.builder()
                                                        .bucketName("sigma-pack-test")
                                                        .prefix("")
                                                        .client(client)
                                                        .build();
    S3MetadataStore store = new S3MetadataStore(config);
    System.out.println(store.getVolumes());
    store.setVolumeSize("test", 100_000_000_000L);
    Thread.sleep(1000);
    System.out.println(store.getVolumeSize("test"));
    System.out.println(store.getVolumes());
  }

  public S3MetadataStore(S3MetadataStoreConfig config) {
    _client = config.getClient();
    _bucketName = config.getBucketName();
    _prefix = config.getPrefix();
  }

  @Override
  public List<String> getVolumes() throws Exception {
    List<String> volumes = new ArrayList<>();
    ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
    listObjectsV2Request.setPrefix(_prefix);
    listObjectsV2Request.setDelimiter("/");
    listObjectsV2Request.setBucketName(_bucketName);
    ListObjectsV2Result result = _client.listObjectsV2(listObjectsV2Request);
    List<S3ObjectSummary> list = result.getObjectSummaries();
    for (S3ObjectSummary summary : list) {
      volumes.add(summary.getKey()
                         .replace("/", ""));
    }
    return volumes;
  }

  @Override
  public long getVolumeSize(String volumeName) throws Exception {
    String key = getKey(volumeName);
    String content = _client.getObjectAsString(_bucketName, key);
    return Long.parseLong(content);
  }

  @Override
  public void setVolumeSize(String volumeName, long length) throws Exception {
    String key = getKey(volumeName);
    _client.putObject(_bucketName, key, Long.toString(length));
  }

  private String getKey(String volumeName) {
    if (_prefix == null || _prefix.isEmpty()) {
      return volumeName;
    } else {
      return KEY_JOINER.join(_prefix, volumeName);
    }
  }

}
