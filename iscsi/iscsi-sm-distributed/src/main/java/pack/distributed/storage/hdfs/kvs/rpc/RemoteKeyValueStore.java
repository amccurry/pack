package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

@KerberosInfo(serverPrincipal = RemoteKeyValueStore.REMOTE_KEY_VALUE_STORE_KERBEROS_PRINCIPAL_KEY)
// @KerberosInfo(serverPrincipal =
// DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(protocolName = "pack.distributed.storage.hdfs.kvs.rpc.RemoteKeyValueStore", protocolVersion = 1)
public interface RemoteKeyValueStore {

  public static String REMOTE_KEY_VALUE_STORE_KERBEROS_PRINCIPAL_KEY = "remote.key.value.store.kerberos.principal";

  StoreList storeList() throws IOException;

  ScanResult scan(String store, Key key) throws IOException;

  Key lastKey(String store) throws IOException;

  GetResult get(String store, Key key) throws IOException;

  TransId put(String store, Key key, Key value) throws IOException;

  TransId delete(String store, Key key) throws IOException;

  TransId deleteRange(String store, Key fromInclusive, Key toExclusive) throws IOException;

  void sync(String store, TransId transId) throws IOException;

}
