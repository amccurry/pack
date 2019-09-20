package pack.iscsi.file.block.storage;

import java.io.IOException;
import java.util.List;

import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;

public class LocalVolumeStore implements PackVolumeStore {

  @Override
  public List<String> getAllVolumes() throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public List<String> getAssignedVolumes() throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void createVolume(String name, long lengthInBytes, int blockSizeInBytes) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void deleteVolume(String name) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void growVolume(String name, long newLengthInBytes) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void assignVolume(String name) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void unassignVolume(String name) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(String name) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public PackVolumeMetadata getVolumeMetadata(long volumeId) throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void renameVolume(String name, String newName) throws IOException {
    throw new RuntimeException("not impl");
  }
}
