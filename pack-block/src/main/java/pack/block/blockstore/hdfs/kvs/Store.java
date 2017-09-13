/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pack.block.blockstore.hdfs.kvs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;

import pack.block.blockstore.hdfs.file.ReadRequest;

public interface Store extends Closeable {

  boolean isOwner() throws IOException;

  void sync(boolean sync) throws IOException;

  Iterable<Entry<Long, ByteBuffer>> scan(Long key) throws IOException;

  void put(long key, ByteBuffer value) throws IOException;

  boolean get(long key, ByteBuffer value) throws IOException;

  /**
   * Return true if more read results remain.
   * @param requests
   * @return
   * @throws IOException
   */
  boolean get(List<ReadRequest> requests) throws IOException;

  void delete(long key) throws IOException;

  void close() throws IOException;

  void writeExternal(ExternalWriter storeWriter, boolean removeKeyValuesOnClose) throws IOException;

  long getSizeOfData();

  int getNumberOfEntries();

  void flush(boolean sync) throws IOException;

}