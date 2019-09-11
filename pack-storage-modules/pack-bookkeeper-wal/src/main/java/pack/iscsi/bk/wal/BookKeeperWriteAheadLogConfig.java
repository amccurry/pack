package pack.iscsi.bk.wal;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.curator.framework.CuratorFramework;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BookKeeperWriteAheadLogConfig {

  BookKeeper bookKeeper;

  CuratorFramework curatorFramework;

  @Builder.Default
  long expireAfterAccess = 1;

  @Builder.Default
  TimeUnit expireAfterAccessTimeUnit = TimeUnit.MINUTES;

  @Builder.Default
  final DigestType digestType = DigestType.MAC;

  @Builder.Default
  int ensSize = 3;

  @Builder.Default
  int writeQuorumSize = 2;

  @Builder.Default
  int ackQuorumSize = 2;

}
