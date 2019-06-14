package pack.s3;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

import jnr.ffi.Pointer;

public class S3FileHandle implements FileHandle {

  private final AmazonS3 _client;
  private final long _blockSize;
  private final String _bucket;
  private final File _cacheDir;
  private final String _volumeName;
  private final LoadingCache<String, File> _cache;

  public S3FileHandle(String bucket, String volumeName, long blockSize, String cache) {
    _cacheDir = new File(cache);
    _bucket = bucket;
    _volumeName = volumeName;
    _blockSize = blockSize;
    _client = AmazonS3ClientBuilder.defaultClient();
    RemovalListener<String, File> listener = notification -> _client.putObject(bucket, notification.getKey(),
        notification.getValue());
    CacheLoader<String, File> loader = new CacheLoader<String, File>() {
      @Override
      public File load(String key) throws Exception {
        GetObjectRequest getObjectRequest = new GetObjectRequest(_bucket, key);
        File file = new File(_cacheDir, key);
        try {
          _client.getObject(getObjectRequest, file);
        } catch (AmazonS3Exception e) {
          if (e.getErrorCode()
               .equals("NoSuchKey")) {
            file.getParentFile()
                .mkdirs();
            try (RandomAccessFile rand = new RandomAccessFile(file, "rw")) {
              rand.setLength(blockSize);
            }
            return file;
          }
          throw e;
        }
        return file;
      }
    };
    _cache = CacheBuilder.newBuilder()
                         .removalListener(listener)
                         .maximumSize(10)
                         .build(loader);
  }

  @Override
  public int read(Pointer buf, int size, long offset) throws Exception {
    String key = getBlockKey(offset);
    File file = _cache.get(key);
    long fileOffset = offset % _blockSize;
    int len = (int) Math.min(size, _blockSize - fileOffset);
    try (RandomAccessFile rand = new RandomAccessFile(file, "r")) {
      rand.seek(fileOffset);
      byte[] buffer = new byte[len];
      rand.readFully(buffer, 0, len);
      buf.put(0, buffer, 0, len);
      return len;
    }
  }

  @Override
  public int write(Pointer buf, int size, long offset) throws Exception {
    String key = getBlockKey(offset);
    File file = _cache.get(key);
    long fileOffset = offset % _blockSize;
    int len = (int) Math.min(size, _blockSize - fileOffset);
    try (RandomAccessFile rand = new RandomAccessFile(file, "rw")) {
      rand.seek(fileOffset);
      byte[] buffer = new byte[len];
      buf.get(0, buffer, 0, len);
      rand.write(buffer, 0, len);
      return len;
    }
  }

  private String getBlockKey(long offset) {
    long blockId = offset / _blockSize;
    return _volumeName + "/" + blockId;
  }

  @Override
  public void close() throws IOException {
    _cache.invalidateAll();
  }

}
