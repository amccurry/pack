package pack.block.util;

import org.ehcache.sizeof.SizeOf;

public interface PackSizeOf {

  public static long getSizeOfObject(Object obj, boolean deep) {
    SizeOf sizeOf = SizeOf.newInstance();
    if (deep) {
      return sizeOf.deepSizeOf(obj);
    } else {
      return sizeOf.sizeOf(obj);
    }
  }

  long getSizeOf();

}
