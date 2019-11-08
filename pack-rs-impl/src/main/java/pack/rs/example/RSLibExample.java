package pack.rs.example;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

public class RSLibExample {

  private static final String RS = "rs";

  public static void main(String[] args) throws IOException {
    int seed = 1;
    int blockSize = 1024;
    int inputSize = 512 * blockSize;
    int dataPartCount = 4;
    int parityPartCount = 2;
    int byteBufferSize = inputSize / dataPartCount;

    Configuration conf = new Configuration();
    ErasureCoderOptions options = new ErasureCoderOptions(dataPartCount, parityPartCount);
    RawErasureEncoder encoder = CodecUtil.createRawEncoder(conf, RS, options);
    RawErasureDecoder decoder = CodecUtil.createRawDecoder(conf, RS, options);

    ByteBuffer data = ByteBuffer.allocateDirect(inputSize);
    ByteBuffer buffer = ByteBuffer.allocateDirect(inputSize);

    Random random = new Random(seed);
    fill(data, random);

    ByteBuffer[] dataParts = getDataParts(data, buffer, dataPartCount, blockSize, byteBufferSize);

    ByteBuffer[] parityParts = new ByteBuffer[parityPartCount];
    for (int i = 0; i < parityPartCount; i++) {
      parityParts[i] = ByteBuffer.allocateDirect(byteBufferSize);
    }
    encoder.encode(dataParts, parityParts);

    int length = 1024;
    int offset = 1024;
    ByteBuffer[] subDataParts = getSubParts(dataParts, offset, length);
    ByteBuffer[] subParityParts = getSubParts(parityParts, offset, length);
    testRead(decoder, subDataParts, subParityParts, length);
  }

  private static ByteBuffer[] getSubParts(ByteBuffer[] dataParts, int offset, int length) {
    ByteBuffer[] subParts = new ByteBuffer[dataParts.length];
    for (int i = 0; i < dataParts.length; i++) {
      ByteBuffer duplicate = dataParts[i].duplicate();
      duplicate.position(offset);
      duplicate.limit(offset + length);
      subParts[i] = duplicate.slice();
    }
    return subParts;
  }

  private static void testRead(RawErasureDecoder decoder, ByteBuffer[] dataParts, ByteBuffer[] parityParts,
      int byteBufferSize) throws IOException {

    ByteBuffer[] inputs = new ByteBuffer[] { dataParts[0], null, dataParts[2], dataParts[3], parityParts[0],
        parityParts[1] };
    reset(inputs);
    int[] erasedIndexes = new int[] { 1 };
    ByteBuffer[] outputs = new ByteBuffer[] { ByteBuffer.allocateDirect(byteBufferSize) };
    decoder.decode(inputs, erasedIndexes, outputs);

    equals(dataParts[1], outputs[0]);
  }

  private static void reset(ByteBuffer... buffer) {
    for (ByteBuffer byteBuffer : buffer) {
      if (byteBuffer != null) {
        byteBuffer.position(0);
      }
    }
  }

  private static void equals(ByteBuffer src, ByteBuffer dst) {
    reset(src, dst);
    if (!src.equals(dst)) {
      throw new RuntimeException("not equal");
    }
  }

  private static ByteBuffer[] getDataParts(ByteBuffer data, ByteBuffer buffer, int parts, int blockSize,
      int byteBufferSize) {
    int sizeLength = data.limit();
    int partsPerBuffer = sizeLength / blockSize;
    int partSize = blockSize / parts;
    ByteBuffer[] buffers = new ByteBuffer[parts];
    int pos = 0;
    for (int i = 0; i < parts; i++) {
      buffer.position(pos);
      buffer.limit(pos + byteBufferSize);
      buffers[i] = buffer.slice();
      fillBuffer(buffers[i], data, i, partsPerBuffer, blockSize, partSize);
      buffers[i].flip();
      pos += byteBufferSize;
    }
    return buffers;
  }

  private static void fillBuffer(ByteBuffer buffer, ByteBuffer data, int bufferId, int partsPerBuffer, int blockSize,
      int partSize) {
    int dataPos = bufferId * partSize;
    for (int part = 0; part < partsPerBuffer; part++) {
      System.out.println("Fill pos " + dataPos + " " + bufferId + " " + part);
      ByteBuffer duplicate = data.duplicate();
      duplicate.position(dataPos);
      duplicate.limit(dataPos + partSize);
      buffer.put(duplicate.slice());
      dataPos += blockSize;
    }

  }

  private static void fill(ByteBuffer data, Random random) {
    byte[] buf = new byte[1024];
    while (data.hasRemaining()) {
      random.nextBytes(buf);
      data.put(buf);
    }
    data.flip();
  }

}
