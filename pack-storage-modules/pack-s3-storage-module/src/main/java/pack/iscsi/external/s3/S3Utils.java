package pack.iscsi.external.s3;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class S3Utils {

  private static final String DATA = "data";
  private static final String VOLUME = "volume";
  private static final String METADATA = "metadata";
  private static final char SEPARATOR = '/';
  private static final Splitter SPLITTER = Splitter.on(SEPARATOR);
  private static final Joiner JOINER = Joiner.on(SEPARATOR);

  public static String getBlockKey(String objectPrefix, long volumeId, long blockId) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(DATA, volumeId, blockId);
    } else {
      return JOINER.join(objectPrefix, DATA, volumeId, blockId);
    }
  }

  public static String getBlockGenerationKey(String objectPrefix, long volumeId, long blockId, long generation) {
    return getBlockKey(objectPrefix, volumeId, blockId) + '/' + generation;
  }

  public static List<String> listObjects(AmazonS3 amazonS3, String bucketName, String prefix) {
    List<String> results = new ArrayList<>();
    ObjectListing listObjects = amazonS3.listObjects(bucketName, prefix);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    for (S3ObjectSummary summary : objectSummaries) {
      results.add(summary.getKey());
    }
    return results;
  }

  public static String getVolumeMetadataKey(String objectPrefix, long volumeId) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(DATA, volumeId, METADATA);
    } else {
      return JOINER.join(objectPrefix, DATA, volumeId, METADATA);
    }
  }

  public static String getVolumeNameKey(String objectPrefix, String name) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(VOLUME, name);
    } else {
      return JOINER.join(objectPrefix, VOLUME, name);
    }
  }

  public static String getVolumeNamePrefix(String objectPrefix) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return VOLUME;
    } else {
      return JOINER.join(objectPrefix, VOLUME);
    }
  }

  public static String getVolumeBlocksPrefix(String objectPrefix, long volumeId) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(DATA, volumeId);
    } else {
      return JOINER.join(objectPrefix, DATA, volumeId);
    }
  }

  public static String getVolumeName(String objectPrefix, String key) {
    List<String> list = SPLITTER.splitToList(key);
    return list.get(list.size() - 1);
  }

}
