/*
 * Copyright (c) 2015 Spotify AB
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.ObservableClient;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.Resolver;
import com.spotify.folsom.client.NotConnectedClient;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.guava.HostAndPort;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvingKetamaClient extends AbstractRawMemcacheClient {
  private static final Logger log = LoggerFactory.getLogger(ResolvingKetamaClient.class);
  public static final int MIN_RESOLVE_WAIT_TIME = 10;
  public static final int MAX_RESOLVE_WAIT_TIME = 3600;

  private final ScheduledExecutorService executor;
  private final Resolver resolver;
  private final long ttl;

  private final Connector connector;
  private final long shutdownDelay;
  private final TimeUnit shutdownUnit;
  private final MyConnectionChangeListener listener = new MyConnectionChangeListener();

  private ScheduledFuture<?> refreshJob;

  private final Object sync = new Object();

  private final Map<HostAndPort, RawMemcacheClient> clients = new HashMap<>();
  private final Collection<RawMemcacheClient> shutdownQueue = new ArrayList<>();
  private volatile RawMemcacheClient currentClient;
  private volatile RawMemcacheClient pendingClient = null;
  private boolean shutdown = false;

  public ResolvingKetamaClient(
      Resolver resolver,
      ScheduledExecutorService executor,
      long period,
      TimeUnit periodUnit,
      final Connector connector,
      long shutdownDelay,
      TimeUnit shutdownUnit) {
    this.resolver = resolver;
    this.connector = connector;
    this.shutdownDelay = shutdownDelay;
    this.shutdownUnit = shutdownUnit;
    this.executor = executor;
    this.currentClient = NotConnectedClient.INSTANCE;
    this.ttl = TimeUnit.SECONDS.convert(period, periodUnit);
  }

  public void start() {
    if (refreshJob != null) {
      throw new RuntimeException("You may only start this once");
    }
    refreshJob = this.executor.schedule(this::resolve, 0, TimeUnit.MILLISECONDS);
  }

  public void resolve() {
    synchronized (sync) {
      if (shutdown) {
        return;
      }
      long ttl = this.ttl; // Default ttl to use if resolve fails
      try {
        final List<Resolver.ResolveResult> lookupResults = resolver.resolve();
        if (lookupResults.isEmpty()) {
          // Just ignore empty results
          return;
        }

        final Set<HostAndPort> newAddresses =
            lookupResults
                .stream()
                .map(result -> HostAndPort.fromParts(result.getHost(), result.getPort()))
                .collect(Collectors.toSet());

        final long resolvedTtl =
            lookupResults
                .stream()
                .mapToLong(Resolver.ResolveResult::getTtl)
                .min()
                .orElse(Long.MAX_VALUE);
        ttl = Math.min(ttl, resolvedTtl);

        final Set<HostAndPort> currentAddresses = clients.keySet();
        if (!newAddresses.equals(currentAddresses)) {

          final ImmutableSet<HostAndPort> toRemove =
              Sets.difference(currentAddresses, newAddresses).immutableCopy();
          final Sets.SetView<HostAndPort> toAdd = Sets.difference(newAddresses, currentAddresses);

          if (!toAdd.isEmpty()) {
            log.info("Connecting to " + toAdd);
          }
          if (!toRemove.isEmpty()) {
            log.info("Scheduling disconnect from " + toRemove);
          }
          for (final HostAndPort host : toAdd) {
            final RawMemcacheClient newClient = connector.connect(host);
            newClient.registerForConnectionChanges(listener);
            clients.put(host, newClient);
          }

          final ImmutableList.Builder<RawMemcacheClient> removedClients = ImmutableList.builder();
          for (final HostAndPort host : toRemove) {
            final RawMemcacheClient removed = clients.remove(host);
            removed.unregisterForConnectionChanges(listener);
            removedClients.add(removed);
          }
          setPendingClient(removedClients);
        }
      } finally {
        long delay = clamp(MIN_RESOLVE_WAIT_TIME, MAX_RESOLVE_WAIT_TIME, ttl);
        refreshJob = this.executor.schedule(this::resolve, delay, TimeUnit.SECONDS);
      }
    }
  }

  private long clamp(int min, int max, long value) {
    return Math.max(min, Math.min(max, value));
  }

  @Override
  public <T> CompletionStage<T> send(Request<T> request) {
    return currentClient.send(request);
  }

  @Override
  public void shutdown() {
    synchronized (sync) {
      shutdown = true;
      if (refreshJob != null) {
        refreshJob.cancel(false);
      }
      clients.values().forEach(RawMemcacheClient::shutdown);
    }
  }

  @Override
  public boolean isConnected() {
    return currentClient.isConnected();
  }

  @Override
  public Throwable getConnectionFailure() {
    return currentClient.getConnectionFailure();
  }

  @Override
  public int numTotalConnections() {
    return currentClient.numTotalConnections();
  }

  @Override
  public int numActiveConnections() {
    return currentClient.numActiveConnections();
  }

  @Override
  public Stream<AddressAndClient> streamNodes() {
    return currentClient.streamNodes();
  }

  public interface Connector {
    RawMemcacheClient connect(HostAndPort input);
  }

  private void setPendingClient(final ImmutableList.Builder<RawMemcacheClient> removedClients) {
    shutdownQueue.addAll(removedClients.build());

    final List<AddressAndClient> addressAndClients =
        clients
            .entrySet()
            .stream()
            .map(e -> new AddressAndClient(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

    // This may invalidate an existing pendingClient but should be fine since it doesn't have any
    // important state of its own.
    final KetamaMemcacheClient newClient = new KetamaMemcacheClient(addressAndClients);
    this.pendingClient = newClient;

    newClient
        .connectFuture()
        .thenRun(
            () -> {
              final ImmutableList<RawMemcacheClient> shutdownJob;
              synchronized (sync) {
                if (pendingClient != newClient) {
                  // We don't care about this event if it's not the expected client
                  return;
                }
                currentClient = newClient;
                pendingClient = null;
                shutdownJob = ImmutableList.copyOf(shutdownQueue);
                shutdownQueue.clear();
              }
              executor.schedule(
                  () -> shutdownJob.forEach(RawMemcacheClient::shutdown),
                  shutdownDelay,
                  shutdownUnit);
              notifyConnectionChange();
            });
  }

  private class MyConnectionChangeListener implements ConnectionChangeListener {
    @Override
    public void connectionChanged(ObservableClient client) {
      notifyConnectionChange();
    }
  }
}
