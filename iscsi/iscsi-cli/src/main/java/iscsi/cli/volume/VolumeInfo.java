package iscsi.cli.volume;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
public class VolumeInfo {

  String name;
  long size;
  int blockSize;
  long actualSize;

}
