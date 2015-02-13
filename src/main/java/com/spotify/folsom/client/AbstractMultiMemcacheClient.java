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

package com.spotify.folsom.client;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.RawMemcacheClient;

import java.util.Collection;
import java.util.List;

public abstract class AbstractMultiMemcacheClient implements RawMemcacheClient {

  protected final Collection<RawMemcacheClient> clients;

  public AbstractMultiMemcacheClient(final Collection<RawMemcacheClient> clients) {
    Preconditions.checkArgument(!clients.isEmpty(), "clients must not be empty");
    this.clients = clients;
  }


  @Override
  public ListenableFuture<Void> shutdown() {
    final List<ListenableFuture<Void>> futures = Lists.newArrayListWithCapacity(clients.size());
    for (final RawMemcacheClient client : clients) {
      futures.add(client.shutdown());
    }

    // ignore our list of nothings :)
    return Utils.transform(Futures.allAsList(futures), new Function<List<Void>, Void>() {
      @Override
      public Void apply(final List<Void> input) {
        return null;
      }
    });
  }

  @Override
  public boolean isConnected() {
    for (final RawMemcacheClient client : clients) {
      if (client.isConnected()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + clients + ")";
  }
}
