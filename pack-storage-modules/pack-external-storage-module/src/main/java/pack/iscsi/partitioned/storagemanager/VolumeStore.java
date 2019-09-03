package pack.iscsi.partitioned.storagemanager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface VolumeStore extends Closeable {

  List<String> getVolumeNames();

  VolumeMetadata getVolumeMetadata(String name) throws IOException;

  VolumeMetadata getVolumeMetadata(long volumeId) throws IOException;

  void createVolume(String name, int blockSize, long lengthInBytes) throws IOException;

  void destroyVolume(String name) throws IOException;

  void renameVolume(String existingName, String newName) throws IOException;

  void growVolume(String name, long lengthInBytes) throws IOException;

  default void close() throws IOException {

  }

}
