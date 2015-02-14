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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.client.MultiRequest;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.GetRequest;
import com.spotify.folsom.client.SetRequest;

import java.util.List;
import java.util.Map;

public class FakeRawMemcacheClient implements RawMemcacheClient {

  private boolean connected = true;
  private final Map<String, byte[]> map = Maps.newHashMap();

  @Override
  public <T> ListenableFuture<T> send(Request<T> request) {
    if (!connected) {
      return Futures.immediateFailedFuture(new MemcacheClosedException());
    }

    if (request instanceof SetRequest) {
      map.put(request.getKey(), ((SetRequest) request).getValue());
      return (ListenableFuture<T>) Futures.<MemcacheStatus>immediateFuture(MemcacheStatus.OK);
    }

    if (request instanceof GetRequest) {
      byte[] value = map.get(request.getKey());
      if (value == null) {
        return (ListenableFuture<T>) Futures.immediateFuture(null);
      }
      return (ListenableFuture<T>) Futures.immediateFuture(GetResult.success(value, 0L));
    }

    if (request instanceof MultiRequest) {
      List<GetResult<byte[]>> result = Lists.newArrayList();
      MultiRequest<?> multiRequest = (MultiRequest<?>) request;
      for (String key : multiRequest.getKeys()) {
        byte[] value = map.get(key);
        if (value != null) {
          result.add(GetResult.success(value, 0));
        } else {
          result.add(null);
        }
      }
      return (ListenableFuture<T>) Futures.<List<GetResult<byte[]>>>immediateFuture(result);
    }

    throw new RuntimeException("Unsupported operation: " + request.getClass());
  }

  @Override
  public ListenableFuture<Void> shutdown() {
    connected = false;
    return Futures.immediateFuture(null);
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  public Map<String, byte[]> getMap() {
    return map;
  }
}
