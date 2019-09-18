package pack.iscsi.spi.wal;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@EqualsAndHashCode
public class BlockJournalRange implements Comparable<BlockJournalRange> {
  long volumeId;
  long blockId;
  String uuid;
  long minGeneration;
  long maxGeneration;

  @Override
  public int compareTo(BlockJournalRange o) {
    return Long.compare(maxGeneration, o.getMaxGeneration());
  }
}
