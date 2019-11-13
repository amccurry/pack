package pack.iscsi.brick.remote.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import io.opentracing.Scope;
import pack.util.tracer.TracerUtil;

public class TracerTransport extends TTransport {

  private final TTransport _transport;

  public TracerTransport(TTransport transport) {
    _transport = transport;
  }

  @Override
  public boolean isOpen() {
    try (Scope scope = TracerUtil.trace(getClass(), "isOpen")) {
      return _transport.isOpen();
    }
  }

  @Override
  public boolean peek() {
    try (Scope scope = TracerUtil.trace(getClass(), "peek")) {
      return _transport.peek();
    }
  }

  @Override
  public void open() throws TTransportException {
    try (Scope scope = TracerUtil.trace(getClass(), "open")) {
      _transport.open();
    }
  }

  @Override
  public void close() {
    try (Scope scope = TracerUtil.trace(getClass(), "close")) {
      _transport.close();
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    try (Scope scope = TracerUtil.trace(getClass(), "read")) {
      return _transport.read(buf, off, len);
    }
  }

  @Override
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    try (Scope scope = TracerUtil.trace(getClass(), "readAll")) {
      return _transport.readAll(buf, off, len);
    }
  }

  @Override
  public void write(byte[] buf) throws TTransportException {
    try (Scope scope = TracerUtil.trace(getClass(), "write")) {
      _transport.write(buf);
    }
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    try (Scope scope = TracerUtil.trace(getClass(), "write")) {
      _transport.write(buf, off, len);
    }
  }

  @Override
  public void flush() throws TTransportException {
    try (Scope scope = TracerUtil.trace(getClass(), "flush")) {
      _transport.flush();
    }
  }

  @Override
  public byte[] getBuffer() {
    return _transport.getBuffer();
  }

  @Override
  public int getBufferPosition() {
    return _transport.getBufferPosition();
  }

  @Override
  public int getBytesRemainingInBuffer() {
    return _transport.getBytesRemainingInBuffer();
  }

  @Override
  public void consumeBuffer(int len) {
    _transport.consumeBuffer(len);
  }

}
