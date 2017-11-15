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
import com.spotify.folsom.GetResult;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.AbstractMultiMemcacheClient;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.MultiRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
  public <T> CompletableFuture<T> send(final Request<T> request) {
    if (request instanceof MultiRequest) {
      // T for Request should always be List<GetResult<byte[]>> here
      // which means that MultiRequest should have T = GetResult<byte[]>
      final MultiRequest<GetResult<byte[]>> multiRequest =
              (MultiRequest<GetResult<byte[]>>) request;
      if (multiRequest.getKeys().size() > 1) {
        return (CompletableFuture<T>) sendSplitRequest(multiRequest);
      }
    }

    return getClient(request.getKey()).send(request);
  }

  private <T> CompletableFuture<List<T>> sendSplitRequest(final MultiRequest<T> multiRequest) {
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

    final Map<RawMemcacheClient, CompletableFuture<List<T>>> futures = Maps.newIdentityHashMap();

    for (final Map.Entry<RawMemcacheClient, List<byte[]>> entry : routing.entrySet()) {
      final List<byte[]> subKeys = entry.getValue();
      final Request<List<T>> subRequest = multiRequest.create(subKeys);
      final RawMemcacheClient client = entry.getKey();
      CompletableFuture<List<T>> send = client.send(subRequest);
      futures.put(client, send);
    }

    CompletableFuture<List<T>> [] cfs = futures.values().toArray(new CompletableFuture[futures.size()]);
    CompletableFuture<List<T>> result = CompletableFuture
        .allOf(cfs)
        .thenApply(__ -> Arrays.stream(cfs)
            .map(CompletableFuture::join)
            .flatMap(List::stream)
            .collect(Collectors.toList()));
    return result;
  }
}
