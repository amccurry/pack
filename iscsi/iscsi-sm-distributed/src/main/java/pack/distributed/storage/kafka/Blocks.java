package pack.distributed.storage.kafka;

import java.util.List;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Blocks {

  List<Block> blocks;

}
