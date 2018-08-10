package pack.block.blockstore.hdfs.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.util.Utils;

public class ImmutableRoaringBitmapManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableRoaringBitmapManager.class);

  private static final String IMMUTABLE_ROARING_BITMAP_MANAGER = "immutable-roaring-bitmap-manager";

  private static final Map<File, OffheapEntry> _entryMap = new ConcurrentHashMap<>();
  private static final Timer TIMER;
  private static AtomicReference<File> _indexDir = new AtomicReference<>();

  static {
    TIMER = new Timer(IMMUTABLE_ROARING_BITMAP_MANAGER, true);
    long period = TimeUnit.SECONDS.toMillis(10);
    TIMER.schedule(getTimerTask(), period, period);
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    ImmutableRoaringBitmapManager.setIndexDir(new File("./index"));
    while (true) {
      RoaringBitmap roaringBitmap = new RoaringBitmap();
      populate(roaringBitmap);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (DataOutputStream dout = new DataOutputStream(out)) {
        roaringBitmap.serialize(dout);
      }
      ImmutableRoaringBitmap bitmap = load("test", new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
      System.out.println(bitmap.getCardinality());
      Thread.sleep(1000);
    }
  }

  private static void populate(RoaringBitmap roaringBitmap) {
    Random random = new Random();
    for (int i = 0; i < 100000; i++) {
      roaringBitmap.add(random.nextInt(10000000));
    }
  }

  public static void setIndexDir(File indexDir) {
    _indexDir.set(indexDir);
    indexDir.mkdirs();
  }

  public static ImmutableRoaringBitmap load(String name, DataInput input) throws IOException {
    RoaringBitmap bitset = new RoaringBitmap();
    bitset.deserialize(input);
    File indexDir = _indexDir.get();
    if (indexDir != null) {
      File file = new File(indexDir, name + "-" + UUID.randomUUID()
                                                      .toString());
      try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
        bitset.serialize(out);
      }
      RandomAccessFile rand = new RandomAccessFile(file, "r");
      MappedByteBuffer byteBuffer = rand.getChannel()
                                        .map(MapMode.READ_ONLY, 0, file.length());
      ImmutableRoaringBitmap immutableRoaringBitmap = new ImmutableRoaringBitmap(byteBuffer);
      _entryMap.put(file, new OffheapEntry(file, rand, byteBuffer, immutableRoaringBitmap));
      return immutableRoaringBitmap;
    } else {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
        bitset.serialize(out);
      }
      return new ImmutableRoaringBitmap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    }
  }

  public static void tryToCleanup() {
    LOGGER.debug("Running index cleanup");
    Iterator<Entry<File, OffheapEntry>> iterator = _entryMap.entrySet()
                                                            .iterator();
    while (iterator.hasNext()) {
      Entry<File, OffheapEntry> entry = iterator.next();
      if (entry.getValue()
               .canCloseDestroyFile()) {
        Utils.close(LOGGER, entry.getValue());
        iterator.remove();
      }
    }
  }

  private static class OffheapEntry implements Closeable {

    private final File _file;
    private final RandomAccessFile _rand;
    private final WeakReference<MappedByteBuffer> _byteBufferRef;
    private final WeakReference<ImmutableRoaringBitmap> _immutableRoaringBitmapRef;

    public OffheapEntry(File file, RandomAccessFile rand, MappedByteBuffer byteBuffer,
        ImmutableRoaringBitmap immutableRoaringBitmap) {
      _file = file;
      _rand = rand;
      _byteBufferRef = new WeakReference<>(byteBuffer);
      _immutableRoaringBitmapRef = new WeakReference<>(immutableRoaringBitmap);
    }

    public boolean canCloseDestroyFile() {
      if (_byteBufferRef.get() != null || _immutableRoaringBitmapRef.get() != null) {
        return false;
      }
      return true;
    }

    @Override
    public void close() throws IOException {
      LOGGER.info("Closing file {}", _file);
      Utils.close(LOGGER, _rand);
      LOGGER.info("Removing file {} {}", _file, _file.delete());
    }

  }

  private static TimerTask getTimerTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          tryToCleanup();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    };
  }
}
