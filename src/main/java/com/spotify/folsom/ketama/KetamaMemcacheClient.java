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

package com.spotify.folsom.ketama;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.AbstractMultiMemcacheClient;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.MultiRequest;
import com.spotify.folsom.client.Utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KetamaMemcacheClient extends AbstractMultiMemcacheClient {

  private static  Collection<RawMemcacheClient> clientsOnly(
    final Collection<AddressAndClient> addressAndClients) {

    int size = addressAndClients.size();
    final List<RawMemcacheClient> clients = Lists.newArrayListWithCapacity(size);
    for (final AddressAndClient client : addressAndClients) {
      clients.add(client.getClient());
    }
    return clients;
  }

  private final Continuum continuum;

  public KetamaMemcacheClient(final Collection<AddressAndClient> clients) {
    super(clientsOnly(clients));

    this.continuum = new Continuum(clients);
  }

  private RawMemcacheClient getClient(final byte[] key) {
    return continuum.findClient(key);
  }

  @Override
  public <T> ListenableFuture<T> send(final Request<T> request) {
    if (request instanceof MultiRequest) {
      // T for Request should always be List<GetResult<byte[]>> here
      // which means that MultiRequest should have T = GetResult<byte[]>
      final MultiRequest<GetResult<byte[]>> multiRequest =
              (MultiRequest<GetResult<byte[]>>) request;
      if (multiRequest.getKeys().size() > 1) {
        return (ListenableFuture<T>) sendSplitRequest(multiRequest);
      }
    }

    return getClient(request.getKey()).send(request);
  }

  private <T> ListenableFuture<List<T>> sendSplitRequest(final MultiRequest<T> multiRequest) {
    final List<byte[]> keys = multiRequest.getKeys();

    final Map<RawMemcacheClient, List<byte[]>> routing = Maps.newIdentityHashMap();
    final List<RawMemcacheClient> routing2 = Lists.newArrayListWithCapacity(keys.size());
    for (final byte[] key : keys) {
      final RawMemcacheClient client = getClient(key);
      List<byte[]> subKeys = routing.get(client);
      if (subKeys == null) {
        subKeys = Lists.newArrayList();
        routing.put(client, subKeys);
      }
      subKeys.add(key);
      routing2.add(client);
    }

    final Map<RawMemcacheClient, ListenableFuture<List<T>>> futures = Maps.newIdentityHashMap();

    for (final Map.Entry<RawMemcacheClient, List<byte[]>> entry : routing.entrySet()) {
      final List<byte[]> subKeys = entry.getValue();
      final Request<List<T>> subRequest = multiRequest.create(subKeys);
      final RawMemcacheClient client = entry.getKey();
      ListenableFuture<List<T>> send = client.send(subRequest);
      futures.put(client, send);
    }
    final ListenableFuture<List<List<T>>> allFutures = Futures.allAsList(futures.values());
    return Utils.transform(allFutures, new Assembler<>(futures, routing2));
  }

  private static class Assembler<T, R> implements Function<List<List<T>>, List<T>> {
    private final Map<R, ListenableFuture<List<T>>> futures;
    private final List<R> routing2;

    public Assembler(Map<R, ListenableFuture<List<T>>> futures, List<R> routing2) {
      this.futures = futures;
      this.routing2 = routing2;
    }

    @Override
    public List<T> apply(final List<List<T>> ignored) {
      final Map<R, Iterator<T>> map = Maps.newIdentityHashMap();
      for (final Map.Entry<R, ListenableFuture<List<T>>> entry : futures.entrySet()) {
        final R client = entry.getKey();
        map.put(client, Futures.getUnchecked(entry.getValue()).iterator());
      }
      final List<T> result = Lists.newArrayList();
      for (final R memcacheClient : routing2) {
        final Iterator<T> iterator = map.get(memcacheClient);
        result.add(iterator.next());
      }
      return result;
    }
  }
}
