package pack.iscsi.brick.remote.thrift;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.transport.TTransport;

import io.opentracing.Scope;
import pack.util.tracer.TracerUtil;

public class TracerTProtocol extends TProtocol {

  private final TProtocol _protocol;

  public TracerTProtocol(TProtocol protocol) {
    super(null);
    _protocol = protocol;
  }

  @Override
  public TTransport getTransport() {
    return _protocol.getTransport();
  }

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeMessageBegin")) {
      _protocol.writeMessageBegin(message);
    }
  }

  @Override
  public void writeMessageEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeMessageEnd")) {
      _protocol.writeMessageEnd();
    }
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeStructBegin")) {
      _protocol.writeStructBegin(struct);
    }
  }

  @Override
  public void writeStructEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeStructEnd")) {
      _protocol.writeStructEnd();
    }
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeFieldBegin")) {
      _protocol.writeFieldBegin(field);
    }
  }

  @Override
  public void writeFieldEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeFieldEnd")) {
      _protocol.writeFieldEnd();
    }
  }

  @Override
  public void writeFieldStop() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeFieldStop")) {
      _protocol.writeFieldStop();
    }
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeMapBegin")) {
      _protocol.writeMapBegin(map);
    }
  }

  @Override
  public void writeMapEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeMapEnd")) {
      _protocol.writeMapEnd();
    }
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeListBegin")) {
      _protocol.writeListBegin(list);
    }
  }

  @Override
  public void writeListEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeListEnd")) {
      _protocol.writeListEnd();
    }
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeSetBegin")) {
      _protocol.writeSetBegin(set);
    }
  }

  @Override
  public void writeSetEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeSetEnd")) {
      _protocol.writeSetEnd();
    }

  }

  @Override
  public void writeBool(boolean b) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeBool")) {
      _protocol.writeBool(b);
    }
  }

  @Override
  public void writeByte(byte b) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeByte")) {
      _protocol.writeByte(b);
    }
  }

  @Override
  public void writeI16(short i16) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeI16")) {
      _protocol.writeI16(i16);
    }
  }

  @Override
  public void writeI32(int i32) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeI32")) {
      _protocol.writeI32(i32);
    }
  }

  @Override
  public void writeI64(long i64) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeI64")) {
      _protocol.writeI64(i64);
    }
  }

  @Override
  public void writeDouble(double dub) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeDouble")) {
      _protocol.writeDouble(dub);
    }
  }

  @Override
  public void writeString(String str) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeString")) {
      _protocol.writeString(str);
    }
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "writeBinary")) {
      _protocol.writeBinary(buf);
    }
  }

  @Override
  public TMessage readMessageBegin() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readMessageBegin")) {
      return _protocol.readMessageBegin();
    }
  }

  @Override
  public void readMessageEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readMessageEnd")) {
      _protocol.readMessageEnd();
    }
  }

  @Override
  public TStruct readStructBegin() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readStructBegin")) {
      return _protocol.readStructBegin();
    }
  }

  @Override
  public void readStructEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readStructEnd")) {
      _protocol.readStructEnd();
    }
  }

  @Override
  public TField readFieldBegin() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readFieldBegin")) {
      return _protocol.readFieldBegin();
    }
  }

  @Override
  public void readFieldEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readFieldEnd")) {
      _protocol.readFieldEnd();
    }
  }

  @Override
  public TMap readMapBegin() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readMapBegin")) {
      return _protocol.readMapBegin();
    }
  }

  @Override
  public void readMapEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readMapEnd")) {
      _protocol.readMapEnd();
    }
  }

  @Override
  public TList readListBegin() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readListBegin")) {
      return _protocol.readListBegin();
    }
  }

  @Override
  public void readListEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readListEnd")) {
      _protocol.readListEnd();
    }
  }

  @Override
  public TSet readSetBegin() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readSetBegin")) {
      return _protocol.readSetBegin();
    }
  }

  @Override
  public void readSetEnd() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readSetEnd")) {
      _protocol.readSetEnd();
    }
  }

  @Override
  public boolean readBool() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readBool")) {
      return _protocol.readBool();
    }

  }

  @Override
  public byte readByte() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readByte")) {
      return _protocol.readByte();
    }
  }

  @Override
  public short readI16() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readI16")) {
      return _protocol.readI16();
    }
  }

  @Override
  public int readI32() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readI32")) {
      return _protocol.readI32();
    }
  }

  @Override
  public long readI64() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readI64")) {
      return _protocol.readI64();
    }

  }

  @Override
  public double readDouble() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readDouble")) {
      return _protocol.readDouble();
    }
  }

  @Override
  public String readString() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readString")) {
      return _protocol.readString();
    }
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    try (Scope scope = TracerUtil.trace(getClass(), "readBinary")) {
      return _protocol.readBinary();
    }
  }

  @Override
  public void reset() {
    try (Scope scope = TracerUtil.trace(getClass(), "reset")) {
      _protocol.reset();
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Class<? extends IScheme> getScheme() {
    try (Scope scope = TracerUtil.trace(getClass(), "getScheme")) {
      return _protocol.getScheme();
    }
  }

}
