/*
 * Copyright (c) 2014-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.folsom;

import com.google.common.base.Throwables;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class EmbeddedServer {

  private final MemCacheDaemon<? extends CacheElement> daemon;
  private final int port;

  public <E extends CacheElement> EmbeddedServer(final boolean binary, final Cache<E> cache) {
    MemCacheDaemon<E> daemon = new MemCacheDaemon<>();
    daemon.setCache(cache);
    daemon.setBinary(binary);
    daemon.setVerbose(false);
    port = findFreePort();
    daemon.setAddr(new InetSocketAddress(this.port));
    daemon.start();
    this.daemon = daemon;
  }

  private static CacheImpl defaultCache() {
    final int maxItems = 1492;
    final int maxBytes = 1024 * 1000;
    final ConcurrentLinkedHashMap<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap.create(
        ConcurrentLinkedHashMap.EvictionPolicy.FIFO, maxItems, maxBytes);
    return new CacheImpl(storage);
  }

  public static int findFreePort() {
    try (ServerSocket tmpSocket = new ServerSocket(0)) {
      return tmpSocket.getLocalPort();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void stop() {
    daemon.stop();
  }

  public int getPort() {
    return port;
  }

  public Cache getCache() {
    return daemon.getCache();
  }
}
