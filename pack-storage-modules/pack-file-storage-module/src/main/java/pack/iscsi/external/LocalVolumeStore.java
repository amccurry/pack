package pack.iscsi.external;

import java.io.IOException;
import java.util.List;

import pack.iscsi.volume.VolumeMetadata;
import pack.iscsi.volume.VolumeStore;

public class LocalVolumeStore implements VolumeStore {

  @Override
  public List<String> getVolumeNames() {
    throw new RuntimeException("Not impl");
  }

  @Override
  public VolumeMetadata getVolumeMetadata(long volumeId) {
    throw new RuntimeException("Not impl");
  }

  @Override
  public void createVolume(String name, int blockSize, long lengthInBytes) throws IOException {
    throw new RuntimeException("Not impl");
  }

  @Override
  public VolumeMetadata getVolumeMetadata(String name) throws IOException {
    throw new RuntimeException("Not impl");
  }

  @Override
  public void destroyVolume(String name) throws IOException {
    throw new RuntimeException("Not impl");
  }

  @Override
  public void renameVolume(String existingName, String newName) throws IOException {
    throw new RuntimeException("Not impl");
  }

  @Override
  public void growVolume(String name, long lengthInBytes) throws IOException {
    throw new RuntimeException("Not impl");
  }

}
