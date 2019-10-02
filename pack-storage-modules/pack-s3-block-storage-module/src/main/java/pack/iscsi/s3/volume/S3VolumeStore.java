package pack.iscsi.s3.volume;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.WeakHashMap;

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.VolumeListener;
import pack.iscsi.spi.block.BlockCacheMetadataStore;

public class S3VolumeStore implements PackVolumeStore, BlockCacheMetadataStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3VolumeStore.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final Random _random = new Random();
  private final String _hostname;
  private final int _maxDeleteBatchSize;
  private final Set<VolumeListener> _listeners;

  public S3VolumeStore(S3VolumeStoreConfig config) {
    _maxDeleteBatchSize = config.getMaxDeleteBatchSize();
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
    _hostname = config.getHostname();
    _listeners = Collections.newSetFromMap(new WeakHashMap<>());
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(String name) throws IOException {
    long volumeId = getVolumeIdInternal(name);
    if (volumeId < 0) {
      return null;
    }
    return getVolumeMetadata(volumeId);
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
    String key = getVolumeMetadataKey(volumeId);
    try {
      String json = _consistentAmazonS3.getObjectAsString(_bucket, key);
      return OBJECT_MAPPER.readValue(json, PackVolumeMetadata.class);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public void renameVolume(String existingName, String newName) throws IOException {
    checkExistence(existingName);
    checkNonExistence(newName);
    checkNotAssigned(existingName);
    PackVolumeMetadata metadata = getVolumeMetadata(existingName);
    long volumeId = metadata.getVolumeId();
    createVolumeNamePointer(newName, volumeId);
    deleteVolumeNamePointer(existingName);
  }

  private String getVolumeMetadataKey(long volumeId) {
    return S3Utils.getVolumeMetadataKey(_objectPrefix, volumeId);
  }

  private String getVolumeNameKey(String name) {
    return S3Utils.getVolumeNameKey(_objectPrefix, name);
  }

  private long createVolumeId(String name) throws IOException {
    while (true) {
      long volumeId = createNewRandomId();
      PackVolumeMetadata volumeMetadata = getVolumeMetadata(volumeId);
      if (volumeMetadata == null) {
        return volumeId;
      }
    }
  }

  private long createNewRandomId() {
    synchronized (_random) {
      return Math.abs(_random.nextLong());
    }
  }

  private void createVolumeNamePointer(String name, long volumeId) {
    _consistentAmazonS3.putObject(_bucket, getVolumeNameKey(name), Long.toString(volumeId));
  }

  private void deleteVolumeNamePointer(String name) {
    _consistentAmazonS3.deleteObject(_bucket, getVolumeNameKey(name));
  }

  private long getVolumeIdInternal(String name) throws IOException {
    String key = getVolumeNameKey(name);
    try {
      String volumeIdString = _consistentAmazonS3.getObjectAsString(_bucket, key);
      return Long.parseLong(volumeIdString);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) {
        return -1L;
      }
      throw e;
    }
  }

  @Override
  public List<String> getAllVolumes() throws IOException {
    List<String> result = new ArrayList<>();
    AmazonS3 client = _consistentAmazonS3.getClient();
    String prefix = S3Utils.getVolumeNamePrefix(_objectPrefix);
    S3Utils.listObjects(client, _bucket, prefix,
        summary -> result.add(S3Utils.getVolumeName(_objectPrefix, summary.getKey())));
    return result;
  }

  @Override
  public void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException {
    checkNonExistence(name);
    long volumeId = createVolumeId(name);
    PackVolumeMetadata metadata = PackVolumeMetadata.builder()
                                                    .name(name)
                                                    .blockSizeInBytes(blockSizeInBytes)
                                                    .lengthInBytes(lengthInBytes)
                                                    .volumeId(volumeId)
                                                    .build();
    String key = getVolumeMetadataKey(metadata.getVolumeId());
    writeVolumeMetadata(key, metadata);
    createVolumeNamePointer(name, volumeId);
  }

  private void writeVolumeMetadata(String key, PackVolumeMetadata metadata) throws IOException {
    byte[] bs = OBJECT_MAPPER.writeValueAsBytes(metadata);
    putByteArray(bs, key);
  }

  @Override
  public void deleteVolume(String name) throws IOException {
    checkExistence(name);
    checkNotAssigned(name);
    checkNoSnapshots(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    long volumeId = metadata.getVolumeId();
    _consistentAmazonS3.deleteObject(_bucket, S3Utils.getVolumeNameKey(_objectPrefix, name));
    _consistentAmazonS3.deleteObject(_bucket, S3Utils.getCachedBlockInfo(_objectPrefix, volumeId));
    _consistentAmazonS3.deleteObject(_bucket, S3Utils.getVolumeMetadataKey(_objectPrefix, volumeId));

    String blockPrefix = S3Utils.getVolumeBlocksPrefix(_objectPrefix, metadata.getVolumeId());
    AmazonS3 client = _consistentAmazonS3.getClient();
    S3Utils.deleteObjects(client, _bucket, blockPrefix, _maxDeleteBatchSize, LOGGER);
  }

  @Override
  public List<String> getAssignedVolumes() throws IOException {
    List<String> result = new ArrayList<>();
    AmazonS3 client = _consistentAmazonS3.getClient();
    String prefix = S3Utils.getAssignedVolumeNamePrefix(_objectPrefix, _hostname);
    S3Utils.listObjects(client, _bucket, prefix,
        summary -> result.add(S3Utils.getVolumeName(_objectPrefix, summary.getKey())));
    return result;
  }

  @Override
  public void assignVolume(String name) throws IOException {
    checkExistence(name);
    checkNotAssigned(name);
    String key = S3Utils.getAssignedVolumeNameKey(_objectPrefix, _hostname, name);
    _consistentAmazonS3.putObject(_bucket, key, _hostname);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    String metadataKey = getVolumeMetadataKey(metadata.getVolumeId());
    writeVolumeMetadata(metadataKey, metadata.toBuilder()
                                             .assignedHostname(_hostname)
                                             .build());
  }

  @Override
  public void unassignVolume(String name) throws IOException {
    checkExistence(name);
    checkAssigned(name);
    String key = S3Utils.getAssignedVolumeNameKey(_objectPrefix, _hostname, name);
    _consistentAmazonS3.deleteObject(_bucket, key);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    String metadataKey = getVolumeMetadataKey(metadata.getVolumeId());
    writeVolumeMetadata(metadataKey, metadata.toBuilder()
                                             .assignedHostname(null)
                                             .build());
  }

  @Override
  public void growVolume(String name, long newLengthInBytes) throws IOException {
    checkExistence(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    String key = getVolumeMetadataKey(metadata.getVolumeId());
    if (newLengthInBytes < metadata.getLengthInBytes()) {
      throw new IOException(
          "Provided length " + newLengthInBytes + " is smaller than existing length " + metadata.getLengthInBytes());
    }
    PackVolumeMetadata newMetadata = metadata.toBuilder()
                                             .lengthInBytes(newLengthInBytes)
                                             .build();
    String json = OBJECT_MAPPER.writeValueAsString(newMetadata);
    _consistentAmazonS3.putObject(_bucket, key, json);
    for (VolumeListener listener : _listeners) {
      listener.lengthChange(newMetadata);
    }
  }

  @Override
  public void register(VolumeListener listener) {
    _listeners.add(listener);
  }

  @Override
  public void setCachedBlockIds(long volumeId, long... blockIds) throws IOException {
    byte[] bs = toByteArray(blockIds);
    String key = S3Utils.getCachedBlockInfo(_objectPrefix, volumeId);
    putByteArray(bs, key);
  }

  @Override
  public long[] getCachedBlockIds(long volumeId) throws IOException {
    String key = S3Utils.getCachedBlockInfo(_objectPrefix, volumeId);
    try {
      S3Object object = _consistentAmazonS3.getObject(_bucket, key);
      long contentLength = object.getObjectMetadata()
                                 .getContentLength();
      S3ObjectInputStream input = object.getObjectContent();
      return toLongArray(input, (int) (contentLength / 8));
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) {
        return new long[] {};
      }
      throw e;
    }
  }

  @Override
  public void createSnapshot(String name, String snapshotName) throws IOException {
    checkExistence(name);
    checkAssigned(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    long volumeId = metadata.getVolumeId();
    VolumeListener listener = getListener(metadata);
    Map<BlockKey, Long> generations = listener.createSnapshot(metadata);

    // Store block info
    byte[] bs = toByteArray(generations);
    String snapshotBlockInfoKey = S3Utils.getVolumeSnapshotBlockInfoKey(_objectPrefix, volumeId, snapshotName);
    putByteArray(bs, snapshotBlockInfoKey);

    // Store metadata
    String snapshotMetadataKey = S3Utils.getVolumeSnapshotMetadataKey(_objectPrefix, volumeId, snapshotName);
    writeVolumeMetadata(snapshotMetadataKey, metadata.toBuilder()
                                                     .assignedHostname(null)
                                                     .build());

    // Store cached block info
    String existingCachedBlockInfoKey = S3Utils.getCachedBlockInfo(_objectPrefix, volumeId);
    String snapshotCachedBlockInfoKey = S3Utils.getVolumeSnapshotCachedBlockInfoKey(_objectPrefix, volumeId,
        snapshotName);

    S3Utils.copy(_consistentAmazonS3, _bucket, existingCachedBlockInfoKey, snapshotCachedBlockInfoKey);
  }

  private void putByteArray(byte[] bs, String key) throws IOException {
    try (ByteArrayInputStream input = new ByteArrayInputStream(bs)) {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(bs.length);
      _consistentAmazonS3.putObject(_bucket, key, input, objectMetadata);
    }
  }

  private byte[] toByteArray(Map<BlockKey, Long> generations) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(outputStream)) {
      Set<Entry<BlockKey, Long>> entrySet = generations.entrySet();
      output.writeInt(entrySet.size());
      for (Entry<BlockKey, Long> e : entrySet) {
        BlockKey blockKey = e.getKey();
        output.writeLong(blockKey.getBlockId());
        output.writeLong(e.getValue());
      }
    }
    return outputStream.toByteArray();
  }

  @Override
  public List<String> listSnapshots(String name) throws IOException {
    checkExistence(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    long volumeId = metadata.getVolumeId();
    String prefix = S3Utils.getVolumeSnapshotPrefix(_objectPrefix, volumeId);
    AmazonS3 amazonS3 = _consistentAmazonS3.getClient();
    Set<String> snapshots = new TreeSet<>();
    S3Utils.listObjects(amazonS3, _bucket, prefix,
        summary -> snapshots.add(S3Utils.getSnapshotName(_objectPrefix, summary.getKey())));
    return ImmutableList.copyOf(snapshots);
  }

  @Override
  public void deleteSnapshot(String name, String snapshotName) throws IOException {
    checkExistence(name);
    checkAssigned(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    long volumeId = metadata.getVolumeId();
    String prefix = S3Utils.getVolumeSnapshotPrefix(_objectPrefix, volumeId, snapshotName);
    AmazonS3 client = _consistentAmazonS3.getClient();
    S3Utils.deleteObjects(client, _bucket, prefix, _maxDeleteBatchSize, LOGGER);
  }

  @Override
  public void sync(String name) throws IOException {
    checkExistence(name);
    checkAssigned(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    VolumeListener listener = getListener(metadata);
    listener.sync(metadata, true, false);
  }

  @Override
  public void cloneVolume(String name, String existingVolume, String snapshotId) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(String name, String snapshotId) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(long volumeId, String snapshotId) throws IOException {
    throw new RuntimeException("not impl");
  }

  private VolumeListener getListener(PackVolumeMetadata metadata) throws IOException {
    for (VolumeListener listener : _listeners) {
      if (listener.hasVolume(metadata)) {
        return listener;
      }
    }
    throw new IOException("Volume listener for volume " + metadata.getName() + " not found");
  }

  private static long[] toLongArray(InputStream input, int count) throws IOException {
    long[] ids = new long[count];
    try (DataInputStream inputStream = new DataInputStream(input)) {
      for (int i = 0; i < ids.length; i++) {
        ids[i] = inputStream.readLong();
      }
    }
    return ids;
  }

  private byte[] toByteArray(long... longs) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(longs.length * 8);
    for (int i = 0; i < longs.length; i++) {
      byteBuffer.putLong(longs[i]);
    }
    byte[] bs = byteBuffer.array();
    return bs;
  }

}
