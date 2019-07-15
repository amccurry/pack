package pack.block.blockserver;

import java.io.IOException;
import java.util.List;

public interface BlockServer {

  byte[] read(long mountId, long position, int length) throws IOException, BlockServerException;

  void write(long mountId, long position, byte[] data) throws IOException, BlockServerException;

  void delete(long mountId, long position, int length) throws IOException, BlockServerException;

  long mount(String volumeName, String snapshotId) throws IOException, BlockServerException;

  void unmount(long mountId) throws IOException, BlockServerException;

  List<String> volumes() throws IOException, BlockServerException;

  List<String> snapshots(String volumeName) throws IOException, BlockServerException;

  long getSnapshotLength(String volumeName, String snapshotId) throws IOException, BlockServerException;

}
