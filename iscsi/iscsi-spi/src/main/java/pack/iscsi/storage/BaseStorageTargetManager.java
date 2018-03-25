package pack.iscsi.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jscsi.target.Target;
import org.jscsi.target.connection.TargetSession;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import pack.iscsi.storage.utils.PackUtils;

public abstract class BaseStorageTargetManager implements StorageTargetManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseStorageTargetManager.class);

  protected static final String TARGET_PREFIX = "iqn.2018-02.pack";

  protected final ConcurrentMap<String, Target> _targetMap = new ConcurrentHashMap<>();

  @Override
  public Target getTarget(String targetName) {
    return _targetMap.get(targetName);
  }

  @Override
  public String[] getTargetNames() {
    registerNewTargets();
    List<String> list = new ArrayList<>(_targetMap.keySet());
    Collections.sort(list);
    return list.toArray(new String[list.size()]);
  }

  protected synchronized void registerNewTargets() {
    List<String> names = getVolumeNames();
    for (String name : names) {
      String fullName = getFullName(name);
      if (!isValidTarget(fullName)) {
        try {
          IStorageModule module = createNewStorageModule(name);
          register(name, name, module);
        } catch (IOException e) {
          LOGGER.error("Error creating new storage module " + name, e);
        }
      }
    }
  }

  @Override
  public boolean isValidTarget(String targetName) {
    return _targetMap.containsKey(targetName);
  }

  @Override
  public synchronized void register(String name, String alias, IStorageModule module) throws IOException {
    String fullName = getFullName(name);
    if (isValidTarget(fullName)) {
      throw new IOException("Already registered " + fullName);
    }
    Target target = new Target(fullName, alias, module);
    _targetMap.put(fullName, target);
  }

  @Override
  public String getFullName(String name) {
    return getTargetPrefix() + "." + name;
  }

  protected String getTargetPrefix() {
    return TARGET_PREFIX + "." + getType();
  }

  private static final ThreadLocal<InternalSession> _session = new ThreadLocal<>();

  private static class InternalSession {
    final Closer closer = Closer.create();
    final TargetSession session;
    final AtomicBoolean writable = new AtomicBoolean();

    InternalSession(TargetSession session) {
      this.session = session;
    }

  }

  public static boolean isSessionWritable() {
    return _session.get().writable.get();
  }

  public static void setSessionWritable(boolean valid) {
    _session.get().writable.set(valid);
  }

  public static TargetSession getSession() {
    return _session.get().session;
  }

  public static Closer getCloser() {
    return _session.get().closer;
  }

  public static void startSession(TargetSession session) {
    _session.set(new InternalSession(session));
  }

  public static void endSession(TargetSession session) {
    InternalSession internalSession = _session.get();
    _session.set(null);
    PackUtils.close(LOGGER, internalSession.closer);
  }

}
