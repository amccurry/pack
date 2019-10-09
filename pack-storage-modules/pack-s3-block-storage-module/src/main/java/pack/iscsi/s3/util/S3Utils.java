package pack.iscsi.s3.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.PackVolumeMetadata;

public class S3Utils {

  private static final String BLOCK_INFO = "block-info";
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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static interface ListResultProcessor {

    void addResult(S3ObjectSummary summary);

  }

  public interface ReadVolumeSnapshotBlockInfo {

    void read(long blockId, long generation) throws IOException;

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

  public static void deleteObjects(AmazonS3 amazonS3, String bucketName, String prefix, int maxDeleteBatchSize,
      Logger logger) {
    List<KeyVersion> keys = new ArrayList<>();
    S3Utils.listObjects(amazonS3, bucketName, prefix, summary -> {
      if (keys.size() >= maxDeleteBatchSize) {
        logger.info("Batch delete of {} keys", keys.size());
        amazonS3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
        keys.clear();
      }
      String key = summary.getKey();
      logger.info("Adding key {} to delete batch", key);
      keys.add(new KeyVersion(key));
    });
    if (keys.size() > 0) {
      logger.info("Batch delete of {} keys", keys.size());
      amazonS3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
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

  public static String getAttachedVolumeNameKey(String objectPrefix, String hostname, String name) {
    return getAttachedVolumeNamePrefix(objectPrefix, hostname) + name;
  }

  public static String getAttachedVolumeNamePrefix(String objectPrefix, String hostname) {
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

  public static String getVolumeSnapshotBlockInfoKey(String objectPrefix, long volumeId, String snapshot) {
    return getVolumeSnapshotPrefix(objectPrefix, volumeId, snapshot) + BLOCK_INFO;
  }

  public static String getVolumeSnapshotMetadataKey(String objectPrefix, long volumeId, String snapshot) {
    return getVolumeSnapshotPrefix(objectPrefix, volumeId, snapshot) + METADATA;
  }

  public static String getVolumeSnapshotCachedBlockInfoKey(String objectPrefix, long volumeId, String snapshot) {
    return getVolumeSnapshotPrefix(objectPrefix, volumeId, snapshot) + CACHED_BLOCK_INFO;
  }

  public static String getVolumeSnapshotPrefix(String objectPrefix, long volumeId) {
    return join(objectPrefix, VOLUME, volumeId, SNAPSHOT) + SEPARATOR;
  }

  public static String getVolumeSnapshotPrefix(String objectPrefix, long volumeId, String snapshot) {
    return getVolumeSnapshotPrefix(objectPrefix, volumeId) + snapshot + SEPARATOR;
  }

  public static String getVolumeName(String objectPrefix, String key) {
    List<String> list = SPLITTER.splitToList(key);
    return list.get(list.size() - 1);
  }

  public static String getSnapshotName(String objectPrefix, String key) {
    List<String> list = SPLITTER.splitToList(key);
    return list.get(list.size() - 2);
  }

  public static long getBlockGenerationFromKey(String blockWithGenerationKey) {
    List<String> parts = getBlockIdAndGenerationParts(blockWithGenerationKey);
    return Long.parseLong(parts.get(1));
  }

  public static long getBlockIdFromKey(String blockWithGenerationKey) {
    List<String> parts = getBlockIdAndGenerationParts(blockWithGenerationKey);
    return Long.parseLong(parts.get(0));
  }

  private static List<String> getBlockIdAndGenerationParts(String blockWithGenerationKey) {
    int indexOfBlockMarker = blockWithGenerationKey.indexOf("/block/") + "/block/".length();
    String str = blockWithGenerationKey.substring(indexOfBlockMarker);
    List<String> list = SPLITTER.splitToList(str);
    return list;
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

  public static void copy(ConsistentAmazonS3 consistentAmazonS3, String bucket, String src, String dst) {
    S3Object s3Object = consistentAmazonS3.getObject(bucket, src);
    long length = s3Object.getObjectMetadata()
                          .getContentLength();
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(length);
    consistentAmazonS3.putObject(bucket, dst, s3Object.getObjectContent(), metadata);
  }

  public static void putByteArray(ConsistentAmazonS3 consistentAmazonS3, String bucket, String key, byte[] bs)
      throws IOException {
    try (ByteArrayInputStream input = new ByteArrayInputStream(bs)) {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(bs.length);
      consistentAmazonS3.putObject(bucket, key, input, objectMetadata);
    }
  }

  public static byte[] getByteArray(ConsistentAmazonS3 consistentAmazonS3, String bucket, String key)
      throws IOException {
    S3Object object = consistentAmazonS3.getObject(bucket, key);
    try (InputStream input = object.getObjectContent()) {
      return IOUtils.toByteArray(input);
    }
  }

  public static void writeVolumeMetadata(ConsistentAmazonS3 consistentAmazonS3, String bucket, String key,
      PackVolumeMetadata metadata) throws IOException {
    byte[] bs = OBJECT_MAPPER.writeValueAsBytes(metadata);
    S3Utils.putByteArray(consistentAmazonS3, bucket, key, bs);
  }

  public static PackVolumeMetadata readVolumeMetadata(ConsistentAmazonS3 consistentAmazonS3, String bucket, String key)
      throws IOException {
    try {
      String json = consistentAmazonS3.getObjectAsString(bucket, key);
      return OBJECT_MAPPER.readValue(json, PackVolumeMetadata.class);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) {
        return null;
      }
      throw e;
    }
  }

  public static void readVolumeSnapshotBlockInfo(ConsistentAmazonS3 consistentAmazonS3, String bucket,
      String snapshotBlockInfoKey, ReadVolumeSnapshotBlockInfo readVolumeSnapshotBlockInfo) throws IOException {
    byte[] bs = S3Utils.getByteArray(consistentAmazonS3, bucket, snapshotBlockInfoKey);
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bs))) {
      int count = input.readInt();
      for (int i = 0; i < count; i++) {
        long blockId = input.readLong();
        long generation = input.readLong();
        if (generation == 0) {
          continue;
        }
        readVolumeSnapshotBlockInfo.read(blockId, generation);
      }
    }
  }

}
