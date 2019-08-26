package pack.iscsi.s3.v1;

import java.nio.ByteBuffer;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3BlockMetadata {

  long generation;
  S3BlockState state;

  public ByteBuffer toByteBuffer() {
    // TODO Auto-generated method stub
    return null;
  }

}
