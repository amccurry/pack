package pack.iscsi.s3.util;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public class S3Utils {

  private static final String NAME = "name";
  private static final String VOLUME = "volume";
  private static final String SNAPSHOT = "snapshot";
  private static final String CACHED_BLOCK_INFO = "cached-block-info";
  private static final String ATTACHMENT = "attachment";
  private static final String METADATA = "metadata";
  private static final char SEPARATOR = '/';
  private static final String BLOCK = "block";
  private static final Splitter SPLITTER = Splitter.on(SEPARATOR);
  private static final Joiner JOINER = Joiner.on(SEPARATOR);

  public static interface ListResultProcessor {

    void addResult(S3ObjectSummary summary);

  }

  public static void listObjects(AmazonS3 amazonS3, String bucketName, String prefix, ListResultProcessor processor) {
    ObjectListing listObjects = amazonS3.listObjects(bucketName, prefix);
    processSummaries(processor, listObjects.getObjectSummaries());
    while (listObjects.isTruncated()) {
      listObjects = amazonS3.listNextBatchOfObjects(listObjects);
      processSummaries(processor, listObjects.getObjectSummaries());
    }
  }

  private static void processSummaries(ListResultProcessor processor, List<S3ObjectSummary> objectSummaries) {
    for (S3ObjectSummary summary : objectSummaries) {
      processor.addResult(summary);
    }
  }

  public static String getVolumeMetadataKey(String objectPrefix, long volumeId) {
    return join(objectPrefix, VOLUME, volumeId, METADATA);
  }

  public static String getCachedBlockInfo(String objectPrefix, long volumeId) {
    return join(objectPrefix, VOLUME, volumeId, CACHED_BLOCK_INFO);
  }

  public static String getVolumeNameKey(String objectPrefix, String name) {
    return getVolumeNamePrefix(objectPrefix) + name;
  }

  public static String getVolumeNamePrefix(String objectPrefix) {
    return join(objectPrefix, NAME) + SEPARATOR;
  }

  public static String getAssignedVolumeNameKey(String objectPrefix, String hostname, String name) {
    return getAssignedVolumeNamePrefix(objectPrefix, hostname) + name;
  }

  public static String getAssignedVolumeNamePrefix(String objectPrefix, String hostname) {
    return join(objectPrefix, ATTACHMENT, hostname) + SEPARATOR;
  }

  public static String getVolumeBlocksPrefix(String objectPrefix, long volumeId) {
    return join(objectPrefix, VOLUME, volumeId, BLOCK) + SEPARATOR;
  }

  public static String getBlockGenerationKeyPrefix(String objectPrefix, long volumeId, long blockId) {
    return getVolumeBlocksPrefix(objectPrefix, volumeId) + blockId + SEPARATOR;
  }

  public static String getBlockGenerationKey(String objectPrefix, long volumeId, long blockId, long generation) {
    return getBlockGenerationKeyPrefix(objectPrefix, volumeId, blockId) + generation;
  }

  public static String getVolumeSnapshotKey(String objectPrefix, long volumeId, String snapshot) {
    return getVolumeSnapshotPrefix(objectPrefix, volumeId) + snapshot;
  }

  public static String getVolumeSnapshotPrefix(String objectPrefix, long volumeId) {
    return join(objectPrefix, VOLUME, volumeId, SNAPSHOT) + SEPARATOR;
  }

  public static String getVolumeName(String objectPrefix, String key) {
    List<String> list = SPLITTER.splitToList(key);
    return list.get(list.size() - 1);
  }

  public static String getSnapshotName(String objectPrefix, String key) {
    List<String> list = SPLITTER.splitToList(key);
    return list.get(list.size() - 1);
  }

  private static String join(String objectPrefix, Object... parts) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(parts);
    } else {
      return JOINER.join(ImmutableList.builder()
                                      .add(objectPrefix)
                                      .addAll(Arrays.asList(parts))
                                      .build());
    }
  }
}
