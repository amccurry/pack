package pack.iscsi.block;

import java.io.File;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class LocalBlockStateStoreConfig {

  File blockStateDir;

}
