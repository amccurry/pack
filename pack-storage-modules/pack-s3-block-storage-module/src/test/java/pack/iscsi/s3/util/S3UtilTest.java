package pack.iscsi.s3.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class S3UtilTest {

  @Test
  public void getVolumeMetadataKey() {
    assertEquals("volume/12345/metadata", S3Utils.getVolumeMetadataKey(null, 12345L));
    assertEquals("prefix/volume/12345/metadata", S3Utils.getVolumeMetadataKey("prefix", 12345L));
  }

  @Test
  public void getCachedBlockInfo() {
    assertEquals("volume/12345/cached-block-info", S3Utils.getCachedBlockInfo(null, 12345L));
    assertEquals("prefix/volume/12345/cached-block-info", S3Utils.getCachedBlockInfo("prefix", 12345L));
  }

  @Test
  public void getVolumeNameKey() {
    assertEquals("name/name", S3Utils.getVolumeNameKey(null, "name"));
    assertEquals("prefix/name/name", S3Utils.getVolumeNameKey("prefix", "name"));
  }

  @Test
  public void getBlockGenerationKeyPrefix() {
    assertEquals("volume/12345/block/54321/", S3Utils.getBlockGenerationKeyPrefix(null, 12345L, 54321L));
    assertEquals("prefix/volume/12345/block/54321/", S3Utils.getBlockGenerationKeyPrefix("prefix", 12345L, 54321L));
  }

  @Test
  public void getVolumeBlocksPrefix() {
    assertEquals("volume/12345/block/", S3Utils.getVolumeBlocksPrefix(null, 12345L));
    assertEquals("prefix/volume/12345/block/", S3Utils.getVolumeBlocksPrefix("prefix", 12345L));
  }

  @Test
  public void getVolumeNamePrefix() {
    assertEquals("name/", S3Utils.getVolumeNamePrefix(null));
    assertEquals("prefix/name/", S3Utils.getVolumeNamePrefix("prefix"));
  }

  @Test
  public void getAssignedVolumeNamePrefix() {
    assertEquals("attachment/host/", S3Utils.getAssignedVolumeNamePrefix(null, "host"));
    assertEquals("prefix/attachment/host/", S3Utils.getAssignedVolumeNamePrefix("prefix", "host"));
  }

  @Test
  public void getVolumeSnapshotBlockInfoKey() {
    assertEquals("volume/12345/snapshot/8765/block-info", S3Utils.getVolumeSnapshotBlockInfoKey(null, 12345L, "8765"));
    assertEquals("prefix/volume/12345/snapshot/8765/block-info",
        S3Utils.getVolumeSnapshotBlockInfoKey("prefix", 12345L, "8765"));
  }

  @Test
  public void getVolumeSnapshotMetadataKey() {
    assertEquals("volume/12345/snapshot/8765/metadata", S3Utils.getVolumeSnapshotMetadataKey(null, 12345L, "8765"));
    assertEquals("prefix/volume/12345/snapshot/8765/metadata",
        S3Utils.getVolumeSnapshotMetadataKey("prefix", 12345L, "8765"));
  }

  @Test
  public void getVolumeSnapshotCachedBlockInfoKey() {
    assertEquals("volume/12345/snapshot/8765/cached-block-info",
        S3Utils.getVolumeSnapshotCachedBlockInfoKey(null, 12345L, "8765"));
    assertEquals("prefix/volume/12345/snapshot/8765/cached-block-info",
        S3Utils.getVolumeSnapshotCachedBlockInfoKey("prefix", 12345L, "8765"));
  }

  @Test
  public void getVolumeSnapshotPrefix() {
    assertEquals("volume/12345/snapshot/", S3Utils.getVolumeSnapshotPrefix(null, 12345L));
    assertEquals("prefix/volume/12345/snapshot/", S3Utils.getVolumeSnapshotPrefix("prefix", 12345L));
  }

}
