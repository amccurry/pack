package pack.block.zk;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;

import pack.s3.MetadataStore;

public class ZkMetadataStore implements MetadataStore {

  private final CuratorFramework _client;

  public ZkMetadataStore(CuratorFramework client) {
    _client = client;
  }

  @Override
  public List<String> getVolumes() throws Exception {
    return _client.getChildren()
                  .forPath("/");
  }

  @Override
  public long getVolumeSize(String volumeName) throws Exception {
    byte[] bs = _client.getData()
                       .forPath("/" + volumeName);
    return Long.parseLong(new String(bs));
  }

  @Override
  public void setVolumeSize(String volumeName, long length) throws Exception {
    _client.setData()
           .forPath("/" + volumeName, Long.toString(length)
                                          .getBytes());
  }

}
