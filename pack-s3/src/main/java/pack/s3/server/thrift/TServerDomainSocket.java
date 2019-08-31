/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package pack.s3.server.thrift;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;

import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import jnr.unixsocket.UnixServerSocket;
import jnr.unixsocket.UnixSocket;
import jnr.unixsocket.UnixSocketAddress;

public class TServerDomainSocket extends TServerTransport {

  private UnixServerSocket serverSocket_ = null;

  public TServerDomainSocket(File path) throws TTransportException {
    path.deleteOnExit();
    try {
      UnixServerSocket serverSocket = new UnixServerSocket();
      SocketAddress endpoint = new UnixSocketAddress(path);
      serverSocket.bind(endpoint);
      serverSocket_ = serverSocket;
    } catch (IOException ioe) {
      close();
      throw new TTransportException("Could not create ServerSocket on path " + path + ".", ioe);
    }
  }

  public void listen() throws TTransportException {

  }

  protected TSocket acceptImpl() throws TTransportException {
    if (serverSocket_ == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
    }
    try {
      UnixSocket result = serverSocket_.accept();
      return new TSocket(result);
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  public void close() {
    if (serverSocket_ != null) {
      serverSocket_ = null;
    }
  }

  public void interrupt() {
    close();
  }

  public UnixServerSocket getServerSocket() {
    return serverSocket_;
  }
}
