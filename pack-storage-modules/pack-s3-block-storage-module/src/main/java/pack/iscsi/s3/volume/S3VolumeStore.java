package pack.iscsi.s3.volume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import consistent.s3.ConsistentAmazonS3;
import pack.iscsi.s3.util.S3Utils;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;

public class S3VolumeStore implements PackVolumeStore {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ConsistentAmazonS3 _consistentAmazonS3;
  private final String _bucket;
  private final String _objectPrefix;
  private final Random _random = new Random();
  private final String _hostname;

  public S3VolumeStore(S3VolumeStoreConfig config) {
    _consistentAmazonS3 = config.getConsistentAmazonS3();
    _bucket = config.getBucket();
    _objectPrefix = config.getObjectPrefix();
    _hostname = config.getHostname();
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
    ObjectListing listObjects = client.listObjects(_bucket, prefix);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    for (S3ObjectSummary summary : objectSummaries) {
      result.add(S3Utils.getVolumeName(_objectPrefix, summary.getKey()));
    }
    return result;
  }

  @Override
  public void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException {
    long volumeId = createVolumeId(name);
    PackVolumeMetadata metadata = PackVolumeMetadata.builder()
                                                    .blockSizeInBytes(blockSizeInBytes)
                                                    .lengthInBytes(lengthInBytes)
                                                    .volumeId(volumeId)
                                                    .build();
    writeVolumeMetadata(metadata);
    createVolumeNamePointer(name, volumeId);
  }

  private void writeVolumeMetadata(PackVolumeMetadata metadata) throws JsonProcessingException {
    String json = OBJECT_MAPPER.writeValueAsString(metadata);
    _consistentAmazonS3.putObject(_bucket, getVolumeMetadataKey(metadata.getVolumeId()), json);
  }

  @Override
  public void deleteVolume(String name) throws IOException {
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    String key = S3Utils.getVolumeNameKey(_objectPrefix, name);
    _consistentAmazonS3.deleteObject(_bucket, key);
    String blockPrefix = S3Utils.getVolumeBlocksPrefix(_objectPrefix, metadata.getVolumeId());
    AmazonS3 client = _consistentAmazonS3.getClient();
    ObjectListing listObjects = client.listObjects(_bucket, blockPrefix);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    List<KeyVersion> keys = new ArrayList<>();
    for (S3ObjectSummary summary : objectSummaries) {
      keys.add(new KeyVersion(summary.getKey()));
    }
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(_bucket);
    deleteObjectsRequest.withKeys(keys);
    client.deleteObjects(deleteObjectsRequest);
  }

  @Override
  public List<String> getAssignedVolumes() throws IOException {
    List<String> result = new ArrayList<>();
    AmazonS3 client = _consistentAmazonS3.getClient();
    String prefix = S3Utils.getAssignedVolumeNamePrefix(_objectPrefix, _hostname);
    ObjectListing listObjects = client.listObjects(_bucket, prefix);
    List<S3ObjectSummary> objectSummaries = listObjects.getObjectSummaries();
    for (S3ObjectSummary summary : objectSummaries) {
      result.add(S3Utils.getVolumeName(_objectPrefix, summary.getKey()));
    }
    return result;
  }

  @Override
  public void assignVolume(String name) throws IOException {
    String key = S3Utils.getAssignedVolumeNameKey(_objectPrefix, _hostname, name);
    _consistentAmazonS3.putObject(_bucket, key, _hostname);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    writeVolumeMetadata(metadata.toBuilder()
                                .assignedHostname(_hostname)
                                .build());
  }

  @Override
  public void unassignVolume(String name) throws IOException {
    String key = S3Utils.getAssignedVolumeNameKey(_objectPrefix, _hostname, name);
    _consistentAmazonS3.deleteObject(_bucket, key);
    PackVolumeMetadata metadata = getVolumeMetadata(name);
    writeVolumeMetadata(metadata.toBuilder()
                                .assignedHostname(null)
                                .build());
  }

  @Override
  public void growVolume(String name, long newLengthInBytes) throws IOException {
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
  }

}
