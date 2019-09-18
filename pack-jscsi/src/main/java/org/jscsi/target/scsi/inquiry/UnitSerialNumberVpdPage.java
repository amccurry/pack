package org.jscsi.target.scsi.inquiry;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.jscsi.target.scsi.IResponseData;
import org.jscsi.target.util.ReadWrite;

public class UnitSerialNumberVpdPage implements IResponseData {

  private static final int HEADER_LENGTH = 4;

  private static final int PAGE_LENGTH_FIELD_INDEX = 2;

  private final byte peripheralQualifierAndPeripheralDeviceType = 0;

  private final byte pageCode = (byte) 0x80;

  private final UUID uuid;

  public UnitSerialNumberVpdPage(UUID uuid) {
    this.uuid = uuid;
  }

  private short getPageLength() {
    return 16;
  }

  public void serialize(ByteBuffer byteBuffer, int index) {
    // serialize header
    byteBuffer.position(index);
    byteBuffer.put(peripheralQualifierAndPeripheralDeviceType);
    byteBuffer.put(pageCode);
    ReadWrite.writeTwoByteInt(byteBuffer, // buffer
        getPageLength(), index + PAGE_LENGTH_FIELD_INDEX);// index
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());
  }

  public int size() {
    return getPageLength() + HEADER_LENGTH;
  }
}