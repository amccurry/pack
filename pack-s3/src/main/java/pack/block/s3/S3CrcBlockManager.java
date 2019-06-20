package pack.block.s3;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import pack.block.CrcBlockManager;

public class S3CrcBlockManager implements CrcBlockManager {

  private final String _bucketName;
  private final AmazonS3 _client;
  private final String _prefix;
  // private final Map<>

  public static void main(String[] args) throws JsonProcessingException {

    Map<Long, Long> map = new HashMap<>();
    map.put(1L, 2L);

    System.out.println(new ObjectMapper().writeValueAsString(map));

  }

  public S3CrcBlockManager(S3CrcBlockManagerConfig config) {
    _client = config.getClient();
    _bucketName = config.getBucketName();
    _prefix = config.getPrefix();
  }

  @Override
  public long getBlockCrc(long blockId) throws Exception {
    return 0;
  }

  @Override
  public void putBlockCrc(long blockId, long crc) throws Exception {

  }

  @Override
  public void sync() {
    // new ObjectMapper().writeValueAsString(value)
  }

  @Override
  public void close() {
    sync();
  }

}
