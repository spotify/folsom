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

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.FlushRequest;
import com.spotify.futures.CompletableFutures;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.concurrent.CompletionStage;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.AbstractMultiMemcacheClient;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.MultiRequest;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
  public <T> CompletionStage<T> send(final Request<T> request) {
    if (request instanceof MultiRequest) {
      // T for Request should always be List<GetResult<byte[]>> here
      // which means that MultiRequest should have T = GetResult<byte[]>
      final MultiRequest<GetResult<byte[]>> multiRequest =
              (MultiRequest<GetResult<byte[]>>) request;
      if (multiRequest.getKeys().size() > 1) {
        return (CompletionStage<T>) sendSplitRequest(multiRequest);
      }
    } else if (request instanceof FlushRequest) {
      return (CompletionStage<T>) sendToAll(request);
    }
    return getClient(request.getKey()).send(request);
  }

  private <T> CompletionStage<MemcacheStatus> sendToAll(final Request<T> request) {
    List<CompletionStage<MemcacheStatus>> futures = clients.stream()
            .map(client -> (CompletionStage<MemcacheStatus>) client.send(request))
            .collect(Collectors.toList());
    return CompletableFutures.allAsList(futures).thenApply(ts ->
        ts.stream()
            .filter(status -> status != MemcacheStatus.OK)
            .findFirst().orElse(MemcacheStatus.OK)
    );
  }

  private <T> CompletionStage<List<T>> sendSplitRequest(final MultiRequest<T> multiRequest) {
    final List<byte[]> keys = multiRequest.getKeys();

    final Map<RawMemcacheClient, List<byte[]>> routing = Maps.newIdentityHashMap();
    final List<RawMemcacheClient> routing2 = Lists.newArrayListWithCapacity(keys.size());
    for (final byte[] key : keys) {
      final RawMemcacheClient client = getClient(key);
      List<byte[]> subKeys = routing.computeIfAbsent(client, k -> Lists.newArrayList());
      subKeys.add(key);
      routing2.add(client);
    }

    final Map<RawMemcacheClient, CompletionStage<List<T>>> futures = Maps.newIdentityHashMap();

    for (final Map.Entry<RawMemcacheClient, List<byte[]>> entry : routing.entrySet()) {
      final List<byte[]> subKeys = entry.getValue();
      final Request<List<T>> subRequest = multiRequest.create(subKeys);
      final RawMemcacheClient client = entry.getKey();
      CompletionStage<List<T>> send = client.send(subRequest);
      futures.put(client, send);
    }
    final Collection<CompletionStage<List<T>>> values = futures.values();
    return CompletableFuture
        .allOf(values.toArray(new CompletableFuture<?>[values.size()]))
        .thenApply(new Assembler<>(futures, routing2));
  }

  private static class Assembler<T, R> implements Function<Void, List<T>> {
    private final Map<R, CompletionStage<List<T>>> futures;
    private final List<R> routing2;

    public Assembler(Map<R, CompletionStage<List<T>>> futures, List<R> routing2) {
      this.futures = futures;
      this.routing2 = routing2;
    }

    @Override
    public List<T> apply(final Void ignored) {
      final Map<R, Iterator<T>> map = Maps.newIdentityHashMap();
      for (final Map.Entry<R, CompletionStage<List<T>>> entry : futures.entrySet()) {
        final R client = entry.getKey();
        map.put(client, CompletableFutures.getCompleted(entry.getValue()).iterator());
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
