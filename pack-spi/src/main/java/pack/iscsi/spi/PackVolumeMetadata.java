package pack.iscsi.spi;

import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

@Value
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(toBuilder = true)
public class PackVolumeMetadata {

  String name;
  long volumeId;
  int blockSizeInBytes;
  long lengthInBytes;
  boolean readOnly;
  List<String> attachedHostnames;

  // Default overrides
  Long syncTimeAfterIdle;
  TimeUnit syncTimeAfterIdleTimeUnit;
  Integer syncExecutorThreadCount;
  Integer readAheadExecutorThreadCount;
  Integer readAheadBlockLimit;

}
