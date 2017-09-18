package jnrfuse.flags;

import java.util.HashSet;
import java.util.Set;

public enum FallocFlags {
  FALLOC_FL_KEEP_SIZE(0x01),

  FALLOC_FL_PUNCH_HOLE(0x02),

  FALLOC_FL_NO_HIDE_STALE(0x04),

  FALLOC_FL_COLLAPSE_RANGE(0x08),

  FALLOC_FL_ZERO_RANGE(0x10),

  FALLOC_FL_INSERT_RANGE(0x20),

  FALLOC_FL_UNSHARE_RANGE(0x40);

  private final int _mask;

  private FallocFlags(int mask) {
    _mask = mask;
  }

  public static Set<FallocFlags> lookup(int mode) {
    Set<FallocFlags> result = new HashSet<>();
    FallocFlags[] values = values();
    for (FallocFlags flag : values) {
      if ((flag._mask & mode) != 0) {
        result.add(flag);
      }
    }
    return result;
  }
}
