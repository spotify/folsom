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

package com.spotify.folsom.roundrobin;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.AbstractMultiMemcacheClient;
import com.spotify.folsom.client.Request;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A client that tries to distribute requests equally to all delegate clients.
 * This would typically only make sense if all the delegates are pointing to the same
 * host/port which might be a reasonable thing to do if the IO operations is a bottleneck.
 */
public class RoundRobinMemcacheClient extends AbstractMultiMemcacheClient {
  private static final int MAX_COUNTER = Integer.MAX_VALUE / 2;

  private final AtomicInteger counter = new AtomicInteger(0);
  private final List<RawMemcacheClient> clients;
  private final int numClients;

  public RoundRobinMemcacheClient(final List<RawMemcacheClient> clients) {
    super(clients);
    this.clients = clients;
    numClients = clients.size();
  }

  @Override
  public <T> ListenableFuture<T> send(final Request<T> request) {
    return getClient().send(request);
  }

  private RawMemcacheClient getClient() {
    int index = counter.incrementAndGet();
    if (index > MAX_COUNTER || index < 0) {
      // To avoid overflow, we reset the counter when it grows too large.
      // There might be a race that makes it reset too often, but it doesn't
      // really matter since it happens so rarely.

      // Don't just set it to zero since that would give bias to the first client.
      index = Math.abs(index) % numClients;
      counter.set(index);
    }
    for (int i = 0; i < numClients; i++) {
      final RawMemcacheClient client = clients.get((index + i) % numClients);
      if (client.isConnected()) {
        return client;
      }
    }
    return clients.get(index % numClients);
  }
}
