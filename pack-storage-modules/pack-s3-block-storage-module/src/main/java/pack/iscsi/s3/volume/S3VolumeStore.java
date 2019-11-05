package pack.iscsi.s3.volume;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.concurrent.ConcurrentUtils;
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
    return S3Utils.readVolumeMetadata(_consistentAmazonS3, _bucket, key);
  }

  @Override
  public void renameVolume(String existingName, String newName) throws IOException {
    checkExistence(existingName);
    checkNonExistence(newName);
    checkDetached(existingName);
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
    checkLength(lengthInBytes);
    checkBlockSize(blockSizeInBytes);
    long volumeId = createVolumeId(name);
    PackVolumeMetadata metadata = PackVolumeMetadata.builder()
                                                    .name(name)
                                                    .blockSizeInBytes(blockSizeInBytes)
                                                    .lengthInBytes(lengthInBytes)
                                                    .volumeId(volumeId)
                                                    .build();
    String key = getVolumeMetadataKey(metadata.getVolumeId());
    S3Utils.writeVolumeMetadata(_consistentAmazonS3, _bucket, key, metadata);
    createVolumeNamePointer(name, volumeId);
  }

  @Override
  public void deleteVolume(String name) throws IOException {
    checkExistence(name);
    checkDetached(name);
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
  public List<String> getAttachedVolumes() throws IOException {
    List<String> result = new ArrayList<>();
    AmazonS3 client = _consistentAmazonS3.getClient();
    String prefix = S3Utils.getAttachedVolumeNamePrefix(_objectPrefix, _hostname);
    S3Utils.listObjects(client, _bucket, prefix,
        summary -> result.add(S3Utils.getVolumeName(_objectPrefix, summary.getKey())));
    return result;
  }

  @Override
  public void attachVolume(String name) throws IOException {
    checkExistence(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    if (!metadata.isReadOnly()) {
      checkDetached(name);
    }
    String key = S3Utils.getAttachedVolumeNameKey(_objectPrefix, _hostname, name);
    _consistentAmazonS3.putObject(_bucket, key, _hostname);
    String metadataKey = getVolumeMetadataKey(metadata.getVolumeId());

    List<String> attachedHostnames = metadata.getAttachedHostnames();
    if (attachedHostnames == null) {
      attachedHostnames = new ArrayList<>();
    }
    if (!attachedHostnames.contains(_hostname)) {
      attachedHostnames.add(_hostname);
    }
    PackVolumeMetadata newMetadata = metadata.toBuilder()
                                             .attachedHostnames(attachedHostnames)
                                             .build();
    S3Utils.writeVolumeMetadata(_consistentAmazonS3, _bucket, metadataKey, newMetadata);
  }

  @Override
  public void detachVolume(String name) throws IOException {
    checkExistence(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    if (!metadata.isReadOnly()) {
      checkAttached(name);
    }
    checkInUse(metadata);
    String key = S3Utils.getAttachedVolumeNameKey(_objectPrefix, _hostname, name);
    _consistentAmazonS3.deleteObject(_bucket, key);

    List<String> attachedHostnames = metadata.getAttachedHostnames();
    if (attachedHostnames == null) {
      attachedHostnames = new ArrayList<>();
    }
    attachedHostnames.remove(_hostname);

    String metadataKey = getVolumeMetadataKey(metadata.getVolumeId());
    PackVolumeMetadata newMetadata = metadata.toBuilder()
                                             .attachedHostnames(attachedHostnames.isEmpty() ? null : attachedHostnames)
                                             .build();
    S3Utils.writeVolumeMetadata(_consistentAmazonS3, _bucket, metadataKey, newMetadata);
  }

  private void checkInUse(PackVolumeMetadata metadata) throws IOException {
    for (VolumeListener listener : _listeners) {
      if (listener.isInUse(metadata)) {
        throw new IOException("Volume " + metadata.getName() + " is still in use.");
      }
    }
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
    S3Utils.putByteArray(_consistentAmazonS3, _bucket, key, bs);
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
  public void createSnapshot(String name, String snapshotId) throws IOException {
    checkExistence(name);
    checkAttached(name);
    checkNoExistence(name, snapshotId);

    PackVolumeMetadata metadata = getVolumeMetadata(name);
    long volumeId = metadata.getVolumeId();
    VolumeListener listener = getListener(metadata);
    Map<BlockKey, Long> generations = listener.createSnapshot(metadata);

    // Store block info
    byte[] bs = toByteArray(generations);
    String snapshotBlockInfoKey = S3Utils.getVolumeSnapshotBlockInfoKey(_objectPrefix, volumeId, snapshotId);
    S3Utils.putByteArray(_consistentAmazonS3, _bucket, snapshotBlockInfoKey, bs);

    // Store metadata
    String snapshotMetadataKey = S3Utils.getVolumeSnapshotMetadataKey(_objectPrefix, volumeId, snapshotId);
    PackVolumeMetadata snapshotMetadata = metadata.toBuilder()
                                                  .attachedHostnames(null)
                                                  .build();
    S3Utils.writeVolumeMetadata(_consistentAmazonS3, _bucket, snapshotMetadataKey, snapshotMetadata);

    // Store cached block info
    String existingCachedBlockInfoKey = S3Utils.getCachedBlockInfo(_objectPrefix, volumeId);
    String snapshotCachedBlockInfoKey = S3Utils.getVolumeSnapshotCachedBlockInfoKey(_objectPrefix, volumeId,
        snapshotId);

    S3Utils.copy(_consistentAmazonS3, _bucket, existingCachedBlockInfoKey, snapshotCachedBlockInfoKey);
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
    checkAttached(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    long volumeId = metadata.getVolumeId();
    String prefix = S3Utils.getVolumeSnapshotPrefix(_objectPrefix, volumeId, snapshotName);
    AmazonS3 client = _consistentAmazonS3.getClient();
    S3Utils.deleteObjects(client, _bucket, prefix, _maxDeleteBatchSize, LOGGER);
  }

  @Override
  public void sync(String name) throws IOException {
    checkExistence(name);
    checkAttached(name);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    VolumeListener listener = getListener(metadata);
    listener.sync(metadata, true, false);
  }

  @Override
  public void cloneVolume(String name, String existingVolume, String snapshotId, boolean readOnly) throws IOException {
    checkNonExistence(name);
    checkExistence(existingVolume);
    checkExistence(existingVolume, snapshotId);

    long cloneVolumeId = createVolumeId(name);
    PackVolumeMetadata existingSnapshotMetadata = getVolumeMetadata(existingVolume, snapshotId);
    long existingVolumeId = existingSnapshotMetadata.getVolumeId();
    PackVolumeMetadata metadata = existingSnapshotMetadata.toBuilder()
                                                          .readOnly(readOnly)
                                                          .volumeId(cloneVolumeId)
                                                          .name(name)
                                                          .build();

    String key = getVolumeMetadataKey(cloneVolumeId);
    S3Utils.writeVolumeMetadata(_consistentAmazonS3, _bucket, key, metadata);
    createVolumeNamePointer(name, cloneVolumeId);
    copyVolumeData(cloneVolumeId, existingVolumeId, snapshotId, metadata.getBlockSizeInBytes());
  }

  private void copyVolumeData(long cloneVolumeId, long existingVolumeId, String snapshotId, int bufferSize)
      throws IOException {
    // metadata already written
    // copy cache block info
    String cloneCachedBlockInfoKey = S3Utils.getCachedBlockInfo(_objectPrefix, cloneVolumeId);
    String snapshotCachedBlockInfoKey = S3Utils.getVolumeSnapshotCachedBlockInfoKey(_objectPrefix, existingVolumeId,
        snapshotId);
    S3Utils.copy(_consistentAmazonS3, _bucket, snapshotCachedBlockInfoKey, cloneCachedBlockInfoKey);
    // copy blocks themselves
    String snapshotBlockInfoKey = S3Utils.getVolumeSnapshotBlockInfoKey(_objectPrefix, existingVolumeId, snapshotId);

    int threads = 10;
    ExecutorService executorService = ConcurrentUtils.executor("copy-" + existingVolumeId, threads);
    List<Future<Void>> futures = new ArrayList<>();
    AtomicLong start = new AtomicLong(System.nanoTime());
    AtomicLong totalTransfered = new AtomicLong();
    AtomicLong objectTotal = new AtomicLong();
    try {
      S3Utils.readVolumeSnapshotBlockInfo(_consistentAmazonS3, _bucket, snapshotBlockInfoKey, (blockId, generation) -> {
        String srcKey = S3Utils.getBlockGenerationKey(_objectPrefix, existingVolumeId, blockId, generation);
        String dstKey = S3Utils.getBlockGenerationKey(_objectPrefix, cloneVolumeId, blockId, generation);
        futures.add(executorService.submit(() -> {
          if (start.get() + TimeUnit.SECONDS.toNanos(5) < System.nanoTime()) {
            LOGGER.info("Clone volume data copy progress, object total {} total bytes {}", objectTotal.get(), totalTransfered.get());
            start.set(System.nanoTime());
          }
          LOGGER.debug("Copying src volumeId {} dst volumeId {} blockId {} generation {}", existingVolumeId,
              cloneVolumeId, blockId, generation);
          S3Utils.copy(_consistentAmazonS3, _bucket, srcKey, dstKey);
          totalTransfered.addAndGet(bufferSize);
          objectTotal.incrementAndGet();
          return null;
        }));
      });
    } finally {
      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        }
      }
      executorService.shutdownNow();
    }
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(String name, String snapshotId) throws IOException {
    long volumeId = getVolumeIdInternal(name);
    if (volumeId < 0) {
      return null;
    }
    return getVolumeMetadata(volumeId, snapshotId);
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(long volumeId, String snapshotId) throws IOException {
    String key = S3Utils.getVolumeSnapshotMetadataKey(_objectPrefix, volumeId, snapshotId);
    return S3Utils.readVolumeMetadata(_consistentAmazonS3, _bucket, key);
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

  @Override
  public void gc(String name) throws IOException {

    // gather all block and generations from snapshots
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    long volumeId = metadata.getVolumeId();
    Map<Long, Set<Long>> snapshotsBlockToGensMap = new HashMap<>();
    List<String> listSnapshots = listSnapshots(name);
    for (String snapshotId : listSnapshots) {
      String snapshotBlockInfoKey = S3Utils.getVolumeSnapshotBlockInfoKey(_objectPrefix, volumeId, snapshotId);
      S3Utils.readVolumeSnapshotBlockInfo(_consistentAmazonS3, _bucket, snapshotBlockInfoKey, (blockId, generation) -> {
        Set<Long> ids = snapshotsBlockToGensMap.get(blockId);
        if (ids == null) {
          snapshotsBlockToGensMap.put(blockId, ids = new HashSet<>());
        }
        ids.add(generation);
      });
    }

    Map<Long, Long> oldestGenerationPerBlock = new HashMap<>();
    String blocksPrefix = S3Utils.getVolumeBlocksPrefix(_objectPrefix, volumeId);
    S3Utils.listObjects(_consistentAmazonS3.getClient(), _bucket, blocksPrefix, summary -> {
      String key = summary.getKey();
      LOGGER.debug("gc processing key {}", key);
      long blockId = S3Utils.getBlockIdFromKey(key);
      long generation = S3Utils.getBlockGenerationFromKey(key);
      if (isOldestGeneration(oldestGenerationPerBlock, blockId, generation)) {
        // keep around might not be the oldest once the block is complete
        Long oldGeneration = oldestGenerationPerBlock.put(blockId, generation);
        if (oldGeneration != null) {
          if (!isInUse(snapshotsBlockToGensMap, blockId, oldGeneration)) {
            String deleteKey = S3Utils.getBlockGenerationKey(_objectPrefix, volumeId, blockId, oldGeneration);
            LOGGER.info("gc delete object {}", deleteKey);
            _consistentAmazonS3.deleteObject(_bucket, deleteKey);
          }
        }
      } else {
        // remove
        if (!isInUse(snapshotsBlockToGensMap, blockId, generation)) {
          LOGGER.info("gc delete object {}", key);
          _consistentAmazonS3.deleteObject(_bucket, key);
        }
      }
    });

  }

  protected boolean isInUse(Map<Long, Set<Long>> snapshotsBlockToGensMap, long blockId, long generation) {
    Set<Long> gens = snapshotsBlockToGensMap.get(blockId);
    if (gens == null) {
      return false;
    }
    return gens.contains(generation);
  }

  protected boolean isOldestGeneration(Map<Long, Long> oldestGenerationPerBlock, long blockId, long generation) {
    Long currentMaxGen = oldestGenerationPerBlock.get(blockId);
    return currentMaxGen == null || generation > currentMaxGen;
  }
}
