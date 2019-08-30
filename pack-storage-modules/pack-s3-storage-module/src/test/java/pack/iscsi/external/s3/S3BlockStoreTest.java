package pack.iscsi.external.s3;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.S3TestSetup;
import pack.iscsi.TestProperties;

public class S3BlockStoreTest {

  @Test
  public void testS3BlockStore() throws Exception {
    ConsistentAmazonS3 consistentAmazonS3 = S3TestSetup.getConsistentAmazonS3();
    String bucket = TestProperties.getBucket();
    String objectPrefix = TestProperties.getObjectPrefix();
    S3BlockStore store = new S3BlockStore(consistentAmazonS3, bucket, objectPrefix);
    long volumeId = store.createVolume("test", 1000, 100000);
    assertTrue(volumeId > S3BlockStore.DEFAULT_STARTING_VOLUME_ID);
  }

}
