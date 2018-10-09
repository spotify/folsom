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
package com.spotify.folsom.retry;

import static com.spotify.folsom.client.Utils.unwrap;

import com.spotify.futures.CompletableFutures;
import java.util.concurrent.CompletionStage;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.MemcacheClosedException;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.Request;

/**
 * A simple wrapping client that retries once (but only for MemcacheClosedException's).
 * This helps avoid some transient problems when a node suddenly stops. It's mostly useful
 * in combination with a client that internally routes to multiple nodes such as
 * the Ketama client or RoundRobin client. It won't prevent all MemcacheClosedException's from
 * propagating, it will just reduce the frequency in some cases.
 *
 * The retrying is intentionally strict about when to retry and how many times to retries in order
 * to minimize risk of causing more problems then it would solve.
 */
public class RetryingClient implements RawMemcacheClient {

  private final RawMemcacheClient delegate;

  public RetryingClient(final RawMemcacheClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public <T> CompletionStage<T> send(final Request<T> request) {
    final CompletionStage<T> future = delegate.send(request);
    return CompletableFutures.exceptionallyCompose(future, e -> {
      e = unwrap(e);
      if (e instanceof MemcacheClosedException && delegate.isConnected()) {
        return delegate.send(request);
      } else {
        return CompletableFutures.exceptionallyCompletedFuture(e);
      }
    });
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public boolean isConnected() {
    return delegate.isConnected();
  }

  @Override
  public Throwable getConnectionFailure() {
    return delegate.getConnectionFailure();
  }

  @Override
  public int numTotalConnections() {
    return delegate.numTotalConnections();
  }

  @Override
  public int numActiveConnections() {
    return delegate.numActiveConnections();
  }

  @Override
  public void registerForConnectionChanges(ConnectionChangeListener listener) {
    delegate.registerForConnectionChanges(listener);
  }

  @Override
  public void unregisterForConnectionChanges(ConnectionChangeListener listener) {
    delegate.unregisterForConnectionChanges(listener);
  }

  @Override
  public String toString() {
    return "Retrying(" + delegate + ")";
  }

}
