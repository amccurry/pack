package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface PackVolumeStore extends Closeable {

  List<String> getAllVolumes() throws IOException;

  List<String> getAssignedVolumes() throws IOException;

  void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException;

  void deleteVolume(String name) throws IOException;

  void growVolume(String name, long newLengthInBytes) throws IOException;

  void renameVolume(String name, String newName) throws IOException;

  void assignVolume(String name) throws IOException;

  void unassignVolume(String name) throws IOException;

  PackVolumeMetadata getVolumeMetadata(String name) throws IOException;

  PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException;

  void createSnapshot(String name, String snapshotName) throws IOException;

  List<String> listSnapshots(String name) throws IOException;

  void deleteSnapshot(String name, String snapshotName) throws IOException;

  void sync(String name) throws IOException;

  default void register(VolumeListener listener) {

  }

  default void checkExistence(String name) throws IOException {
    if (!exists(name)) {
      throw new IOException("Volume " + name + " does not exist");
    }
  }

  default void checkNonExistence(String name) throws IOException {
    if (exists(name)) {
      throw new IOException("Volume " + name + " exists");
    }
  }

  default void checkNotAssigned(String name) throws IOException {
    if (isAssigned(name)) {
      throw new IOException("Volume " + name + " assigned to a host");
    }
  }

  default void checkAssigned(String name) throws IOException {
    if (!isAssigned(name)) {
      throw new IOException("Volume " + name + " not assigned to a host");
    }
  }

  default void checkNewSizeInBytes(String name, long newSizeInBytes) throws IOException {
    PackVolumeMetadata volumeMetadata = getVolumeMetadata(name);
    if (newSizeInBytes <= volumeMetadata.getLengthInBytes()) {
      throw new IOException("Volume " + name + " new size too small");
    }
  }

  default void checkNoSnapshots(String name) throws IOException {
    if (!listSnapshots(name).isEmpty()) {
      throw new IOException("Volume " + name + " has snapshots");
    }
  }

  default boolean isAssigned(String name) throws IOException {
    return getAssignedVolumes().contains(name);
  }

  default boolean exists(String name) throws IOException {
    return getAllVolumes().contains(name);
  }

  default void close() throws IOException {

  }
}
