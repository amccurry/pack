package pack.block.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class S3Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3Utils.class);

  private static final String _404_NOT_FOUND = "404 Not Found";

  public static ObjectMetadata getObjectMetadata(AmazonS3 client, String bucketName, String key, long pollTime) {
    TRY_AGAIN: while (true) {
      try {
        return client.getObjectMetadata(bucketName, key);
      } catch (AmazonS3Exception e) {
        if (e.getErrorCode()
             .equals(_404_NOT_FOUND)) {
          try {
            Thread.sleep(pollTime);
          } catch (InterruptedException ex) {
            LOGGER.error("Unknown error", ex);
            throw e;
          }
          continue TRY_AGAIN;
        }
        throw e;
      }
    }
  }

  public static S3Object getObject(AmazonS3 client, GetObjectRequest getObjectRequest, long pollTime) {
    TRY_AGAIN: while (true) {
      try {
        return client.getObject(getObjectRequest);
      } catch (AmazonS3Exception e) {
        if (e.getErrorCode()
             .equals(_404_NOT_FOUND)) {
          try {
            Thread.sleep(pollTime);
          } catch (InterruptedException ex) {
            LOGGER.error("Unknown error", ex);
            throw e;
          }
          continue TRY_AGAIN;
        }
        throw e;
      }
    }
  }

}
