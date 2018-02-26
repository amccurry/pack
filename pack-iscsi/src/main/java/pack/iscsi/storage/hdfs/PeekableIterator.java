package pack.iscsi.storage.hdfs;

import java.util.Iterator;

public class PeekableIterator<T> implements Iterator<T> {

  private final Iterator<T> _iterator;
  private T _current;

  private PeekableIterator(Iterator<T> iterator, T current) {
    _iterator = iterator;
    _current = current;
  }

  public static <T> PeekableIterator<T> wrap(Iterator<T> iterator) {
    if (iterator.hasNext()) {
      return new PeekableIterator<T>(iterator, iterator.next());
    }
    return new PeekableIterator<T>(iterator, null);
  }

  /**
   * Only valid is hasNext is true. If hasNext if false, peek will return null;
   * 
   * @return <T>
   */
  public T peek() {
    return _current;
  }

  @Override
  public boolean hasNext() {
    if (_current != null) {
      return true;
    }
    return _iterator.hasNext();
  }

  @Override
  public T next() {
    T next = null;
    if (_iterator.hasNext()) {
      next = _iterator.next();
    }
    T result = _current;
    _current = next;
    return result;
  }

}