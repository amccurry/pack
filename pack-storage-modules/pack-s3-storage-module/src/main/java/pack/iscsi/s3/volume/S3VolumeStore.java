package pack.iscsi.s3.volume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.volume.VolumeMetadata;
import pack.iscsi.volume.VolumeStore;

public class S3VolumeStore implements VolumeStore {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final Random _random = new Random();

  public S3VolumeStore(S3VolumeStoreConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
  }

  @Override
  public List<String> getVolumeNames() {
    List<String> result = new ArrayList<>();
    AmazonS3 client = _consistentAmazonS3.getClient();
    String prefix = S3Utils.getVolumeNamePrefix(_objectPrefix);
    ObjectListing listObjects = client.listObjects(_bucket, prefix);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    for (S3ObjectSummary summary : objectSummaries) {
      result.add(S3Utils.getVolumeName(_objectPrefix, summary.getKey()));
    }
    return result;
  }

  @Override
  public VolumeMetadata getVolumeMetadata(String name) throws IOException {
    long volumeId = getVolumeIdInternal(name);
    if (volumeId < 0) {
      return null;
    }
    return getVolumeMetadata(volumeId);
  }

  @Override
  public VolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
    String key = getVolumeMetadataKey(volumeId);
    try {
      String json = _consistentAmazonS3.getObjectAsString(_bucket, key);
      return OBJECT_MAPPER.readValue(json, VolumeMetadata.class);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() == 404) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public void createVolume(String name, int blockSize, long lengthInBytes) throws IOException {
    checkForNonExistence(name);
    long volumeId = createVolumeId(name);
    VolumeMetadata metadata = VolumeMetadata.builder()
                                            .blockSize(blockSize)
                                            .lengthInBytes(lengthInBytes)
                                            .volumeId(volumeId)
                                            .build();
    String json = OBJECT_MAPPER.writeValueAsString(metadata);
    _consistentAmazonS3.putObject(_bucket, getVolumeMetadataKey(volumeId), json);
    createVolumeNamePointer(name, volumeId);
  }

  @Override
  public void destroyVolume(String name) throws IOException {
    checkForExistence(name);
    // VolumeMetadata metadata = getVolumeMetadata(name);
    String key = S3Utils.getVolumeNameKey(_objectPrefix, name);
    _consistentAmazonS3.deleteObject(_bucket, key);
    // String blockPrefix = S3Utils.getVolumeBlocksPrefix(_objectPrefix,
    // metadata.getVolumeId());
    // TODO delete block objects
  }

  @Override
  public void renameVolume(String existingName, String newName) throws IOException {
    checkForExistence(existingName);
    checkForNonExistence(newName);
    VolumeMetadata metadata = getVolumeMetadata(existingName);
    long volumeId = metadata.getVolumeId();
    createVolumeNamePointer(newName, volumeId);
    deleteVolumeNamePointer(existingName);
  }

  @Override
  public void growVolume(String name, long lengthInBytes) throws IOException {
    checkForExistence(name);
    VolumeMetadata metadata = getVolumeMetadata(name);
    String key = getVolumeMetadataKey(metadata.getVolumeId());
    if (lengthInBytes < metadata.getLengthInBytes()) {
      throw new IOException(
          "Provided length " + lengthInBytes + " is smaller than existing length " + metadata.getLengthInBytes());
    }
    VolumeMetadata newMetadata = metadata.toBuilder()
                                         .lengthInBytes(lengthInBytes)
                                         .build();
    String json = OBJECT_MAPPER.writeValueAsString(newMetadata);
    _consistentAmazonS3.putObject(_bucket, key, json);
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
      VolumeMetadata volumeMetadata = getVolumeMetadata(volumeId);
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

  private void checkForExistence(String name) throws IOException {
    if (getVolumeMetadata(name) == null) {
      throw new IOException("Volume " + name + " does not exist");
    }
  }

  private void checkForNonExistence(String name) throws IOException {
    if (getVolumeMetadata(name) != null) {
      throw new IOException("Volume " + name + " already exists");
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

}
