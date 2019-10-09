package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface PackVolumeStore extends Closeable {

  List<String> getAllVolumes() throws IOException;

  List<String> getAttachedVolumes() throws IOException;

  void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException;

  void cloneVolume(String name, String existingVolume, String snapshotId) throws IOException;

  void deleteVolume(String name) throws IOException;

  void growVolume(String name, long newLengthInBytes) throws IOException;

  void renameVolume(String name, String newName) throws IOException;

  void attachVolume(String name) throws IOException;

  void detachVolume(String name) throws IOException;

  PackVolumeMetadata getVolumeMetadata(String name) throws IOException;

  PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException;

  PackVolumeMetadata getVolumeMetadata(String name, String snapshotId) throws IOException;

  PackVolumeMetadata getVolumeMetadata(long volumeId, String snapshotId) throws IOException;

  void createSnapshot(String name, String snapshotId) throws IOException;

  List<String> listSnapshots(String name) throws IOException;

  void deleteSnapshot(String name, String snapshotId) throws IOException;

  void sync(String name) throws IOException;

  void gc(String name) throws IOException;

  default void register(VolumeListener listener) {

  }

  default void checkExistence(String name) throws IOException {
    if (!exists(name)) {
      throw new IOException("Volume " + name + " does not exist");
    }
  }

  default void checkExistence(String name, String snapshotId) throws IOException {
    checkAttached(name);
    if (!listSnapshots(name).contains(snapshotId)) {
      throw new IOException("Volume " + name + " with snapshot " + snapshotId + " does not exist");
    }
  }
  
  default void checkNoExistence(String name, String snapshotId) throws IOException {
    checkAttached(name);
    if (listSnapshots(name).contains(snapshotId)) {
      throw new IOException("Volume " + name + " with snapshot " + snapshotId + " alreadt exists");
    }
  }

  default void checkNonExistence(String name) throws IOException {
    if (exists(name)) {
      throw new IOException("Volume " + name + " exists");
    }
  }

  default void checkDetached(String name) throws IOException {
    if (isAttached(name)) {
      throw new IOException("Volume " + name + " attached to a host");
    }
  }

  default void checkAttached(String name) throws IOException {
    if (!isAttached(name)) {
      throw new IOException("Volume " + name + " not attached to a host");
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

  default boolean isAttached(String name) throws IOException {
    return getAttachedVolumes().contains(name);
  }

  default boolean exists(String name) throws IOException {
    return getAllVolumes().contains(name);
  }

  default void close() throws IOException {

  }
}
