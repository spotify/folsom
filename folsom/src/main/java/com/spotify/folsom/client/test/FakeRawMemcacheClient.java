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
package com.spotify.folsom.client.test;

import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.GetResult;
import com.spotify.folsom.MemcacheClosedException;
import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.Metrics;
import com.spotify.folsom.client.GetRequest;
import com.spotify.folsom.client.MultiRequest;
import com.spotify.folsom.client.NoopMetrics;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.client.SetRequest;
import com.spotify.folsom.client.ascii.DeleteRequest;
import com.spotify.folsom.client.ascii.IncrRequest;
import com.spotify.folsom.client.ascii.TouchRequest;
import com.spotify.folsom.guava.HostAndPort;
import com.spotify.folsom.ketama.AddressAndClient;
import com.spotify.futures.CompletableFutures;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public class FakeRawMemcacheClient extends AbstractRawMemcacheClient {

  private boolean connected = true;
  private final Map<ByteBuffer, byte[]> map = new HashMap<>();
  private int outstanding = 0;
  private final String address;

  public FakeRawMemcacheClient() {
    this(new NoopMetrics());
  }

  public FakeRawMemcacheClient(final Metrics metrics) {
    this(metrics, "address:123");
  }

  public FakeRawMemcacheClient(final Metrics metrics, final String address) {
    metrics.registerOutstandingRequestsGauge(() -> outstanding);
    this.address = address;
  }

  @Override
  public <T> CompletionStage<T> send(Request<T> request) {
    if (!connected) {
      return CompletableFutures.exceptionallyCompletedFuture(
          new MemcacheClosedException("Disconnected"));
    }

    if (request instanceof SetRequest) {
      map.put(ByteBuffer.wrap(request.getKey()), ((SetRequest) request).getValue());
      return (CompletionStage<T>) CompletableFuture.completedFuture(MemcacheStatus.OK);
    }

    if (request instanceof GetRequest) {
      byte[] value = map.get(ByteBuffer.wrap(request.getKey()));
      if (value == null) {
        return CompletableFuture.completedFuture(null);
      }
      return (CompletionStage<T>) CompletableFuture.completedFuture(GetResult.success(value, 0L));
    }

    if (request instanceof MultiRequest) {
      List<GetResult<byte[]>> result = new ArrayList<>();
      MultiRequest<?> multiRequest = (MultiRequest<?>) request;
      for (byte[] key : multiRequest.getKeys()) {
        byte[] value = map.get(ByteBuffer.wrap(key));
        if (value != null) {
          result.add(GetResult.success(value, 0));
        } else {
          result.add(null);
        }
      }
      return (CompletionStage<T>) CompletableFuture.completedFuture(result);
    }

    // Don't actually do anything here
    if (request instanceof TouchRequest) {
      return (CompletionStage<T>) CompletableFuture.completedFuture(MemcacheStatus.OK);
    }

    if (request instanceof IncrRequest) {
      IncrRequest incrRequest = (IncrRequest) request;
      byte[] key = request.getKey();
      byte[] value = map.get(ByteBuffer.wrap(key));
      if (value == null) {
        return (CompletionStage<T>) CompletableFuture.completedFuture(null);
      }
      long longValue = Long.parseLong(new String(value));
      long newValue = longValue + incrRequest.multiplier() * incrRequest.getBy();
      map.put(ByteBuffer.wrap(key), Long.toString(newValue).getBytes());
      return (CompletionStage<T>) CompletableFuture.completedFuture(newValue);
    }

    if (request instanceof DeleteRequest) {
      map.remove(ByteBuffer.wrap(request.getKey()));
      return (CompletionStage<T>) CompletableFuture.completedFuture(MemcacheStatus.OK);
    }

    throw new RuntimeException("Unsupported operation: " + request.getClass());
  }

  @Override
  public void shutdown() {
    connected = false;
    notifyConnectionChange();
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public Throwable getConnectionFailure() {
    return null;
  }

  @Override
  public int numTotalConnections() {
    return 1;
  }

  @Override
  public int numActiveConnections() {
    return connected ? 1 : 0;
  }

  @Override
  public Stream<AddressAndClient> streamNodes() {
    return Stream.of(new AddressAndClient(HostAndPort.fromString(address), this));
  }

  public Map<ByteBuffer, byte[]> getMap() {
    return map;
  }

  public void setOutstandingRequests(int outstanding) {
    this.outstanding = outstanding;
  }

  @Override
  public String toString() {
    return "FakeRawMemcacheClient{" + "address='" + address + '\'' + '}';
  }
}
