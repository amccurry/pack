package pack.iscsi.s3.util;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import pack.iscsi.io.IOUtils;

public class S3Utils {

  private static final String ASSIGNED = "assigned";
  private static final String WAL = "wal";
  private static final String DATA = "data";
  private static final String VOLUME = "volume";
  private static final String METADATA = "metadata";
  private static final char SEPARATOR = '/';
  private static final Splitter SPLITTER = Splitter.on(SEPARATOR);
  private static final Joiner JOINER = Joiner.on(SEPARATOR);

  public static String getBlockKeyPrefix(String objectPrefix, long volumeId, long blockId) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(DATA, volumeId, blockId) + SEPARATOR;
    } else {
      return JOINER.join(objectPrefix, DATA, volumeId, blockId) + SEPARATOR;
    }
  }

  public static String getBlockGenerationKey(String objectPrefix, long volumeId, long blockId, long generation) {
    return getBlockKeyPrefix(objectPrefix, volumeId, blockId) + generation;
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
      return VOLUME + SEPARATOR;
    } else {
      return JOINER.join(objectPrefix, VOLUME) + SEPARATOR;
    }
  }

  public static String getAssignedVolumeNamePrefix(String objectPrefix, String hostname) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(ASSIGNED, hostname) + SEPARATOR;
    } else {
      return JOINER.join(objectPrefix, ASSIGNED, hostname) + SEPARATOR;
    }
  }

  public static String getAssignedVolumeNameKey(String objectPrefix, String hostname, String name) {
    return getAssignedVolumeNamePrefix(objectPrefix, hostname) + name;
  }

  public static String getVolumeBlocksPrefix(String objectPrefix, long volumeId) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(DATA, volumeId) + SEPARATOR;
    } else {
      return JOINER.join(objectPrefix, DATA, volumeId) + SEPARATOR;
    }
  }

  public static String getVolumeName(String objectPrefix, String key) {
    List<String> list = SPLITTER.splitToList(key);
    return list.get(list.size() - 1);
  }

  public static String getWalKey(String objectPrefix, long volumeId, long blockId, long generation) {
    return getWalKeyPrefix(objectPrefix, volumeId, blockId) + IOUtils.toStringWithLeadingZeros(generation);
  }

  public static String getWalKeyPrefix(String objectPrefix, long volumeId, long blockId) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(WAL, volumeId, blockId) + SEPARATOR;
    } else {
      return JOINER.join(objectPrefix, WAL, volumeId, blockId) + SEPARATOR;
    }
  }

  public static long getWalGeneration(String key) {
    List<String> list = SPLITTER.splitToList(key);
    return Long.parseLong(list.get(list.size() - 1));
  }

}
