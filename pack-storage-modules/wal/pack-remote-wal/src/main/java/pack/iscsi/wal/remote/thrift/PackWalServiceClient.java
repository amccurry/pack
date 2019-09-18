package pack.iscsi.wal.remote.thrift;

import java.io.Closeable;

import pack.iscsi.wal.remote.generated.PackWalService.Iface;

public interface PackWalServiceClient extends Iface, Closeable {

}
