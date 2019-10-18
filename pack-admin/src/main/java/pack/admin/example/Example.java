package pack.admin.example;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.admin.PackVolumeAdminServer;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import spark.Service;

public class Example {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(Example.class);

  public static void main(String[] args) throws IOException {
    Service service = Service.ignite();

    String hostname = InetAddress.getLocalHost()
                                 .getHostName();

    Map<String, PackVolumeMetadata> allVolumes = new ConcurrentHashMap<>();
    List<String> attachedVolumes = new ArrayList<String>();

    Map<String, List<String>> snapshots = new ConcurrentHashMap<>();

    PackVolumeStore packAdmin = new PackVolumeStore() {

      @Override
      public List<String> getAllVolumes() throws IOException {
        Set<String> set = allVolumes.keySet();
        List<String> list = new ArrayList<>(set);
        Collections.sort(list);
        return list;
      }

      @Override
      public List<String> getAttachedVolumes() throws IOException {
        return attachedVolumes;
      }

      @Override
      public void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException {
        checkNonExistence(name);
        PackVolumeMetadata metadata = PackVolumeMetadata.builder()
                                                        .blockSizeInBytes(blockSizeInBytes)
                                                        .lengthInBytes(lengthInBytes)
                                                        .name(name)
                                                        .volumeId(new Random().nextLong())
                                                        .build();
        allVolumes.put(name, metadata);
      }

      @Override
      public void deleteVolume(String name) throws IOException {
        checkExistence(name);
        checkDetached(name);
        allVolumes.remove(name);
      }

      @Override
      public void growVolume(String name, long newLengthInBytes) throws IOException {
        checkExistence(name);
        checkNewSizeInBytes(name, newLengthInBytes);
        PackVolumeMetadata metadata = getVolumeMetadata(name);
        allVolumes.put(name, metadata.toBuilder()
                                     .lengthInBytes(newLengthInBytes)
                                     .build());
      }

      @Override
      public void attachVolume(String name) throws IOException {
        checkExistence(name);
        checkDetached(name);
        attachedVolumes.add(name);
        PackVolumeMetadata metadata = getVolumeMetadata(name);
        allVolumes.put(name, metadata.toBuilder()
                                     .attachedHostnames(Arrays.asList(hostname))
                                     .build());
      }

      @Override
      public void detachVolume(String name) throws IOException {
        checkExistence(name);
        checkAttached(name);
        attachedVolumes.remove(name);
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(String name) throws IOException {
        checkExistence(name);
        return allVolumes.get(name);
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
        for (PackVolumeMetadata metadata : allVolumes.values()) {
          if (volumeId == metadata.getVolumeId()) {
            return metadata;
          }
        }
        throw new IOException("Volume id " + volumeId + " does not exist");
      }

      @Override
      public void renameVolume(String name, String newName) throws IOException {
        checkExistence(name);
        checkNonExistence(newName);
        checkDetached(name);
        PackVolumeMetadata metadata = allVolumes.remove(name);
        allVolumes.put(newName, metadata.toBuilder()
                                        .name(newName)
                                        .build());
      }

      @Override
      public void createSnapshot(String name, String snapshotId) throws IOException {
        checkExistence(name);
        checkNoExistence(name, snapshotId);
        List<String> list = snapshots.get(name);
        if (list == null) {
          snapshots.put(name, list = new ArrayList<>());
        }
        list.add(snapshotId);
      }

      @Override
      public List<String> listSnapshots(String name) throws IOException {
        checkExistence(name);
        List<String> list = snapshots.get(name);
        return list == null ? Arrays.asList() : list;
      }

      @Override
      public void deleteSnapshot(String name, String snapshotName) throws IOException {
        LOGGER.info("deleteSnapshot {} {}", name, snapshotName);
      }

      @Override
      public void sync(String name) throws IOException {
        LOGGER.info("sync {}", name);
      }

      @Override
      public void cloneVolume(String name, String existingVolume, String snapshotId, boolean readOnly)
          throws IOException {
        checkNonExistence(name);
        checkExistence(existingVolume);
        checkExistence(existingVolume, snapshotId);
        PackVolumeMetadata metadata = getVolumeMetadata(existingVolume, snapshotId);
        allVolumes.put(name, metadata.toBuilder()
                                     .name(name)
                                     .readOnly(readOnly)
                                     .volumeId(new Random().nextLong())
                                     .attachedHostnames(null)
                                     .build());
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(String name, String snapshotId) throws IOException {
        return getVolumeMetadata(name);
      }

      @Override
      public PackVolumeMetadata getVolumeMetadata(long volumeId, String snapshotId) throws IOException {
        return getVolumeMetadata(volumeId);
      }

      @Override
      public void gc(String name) throws IOException {
        throw new RuntimeException("not impl");
      }

    };
    PackVolumeAdminServer server = new PackVolumeAdminServer(service, packAdmin);
    server.setup();
  }
}
