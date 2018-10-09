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

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.spotify.folsom.guava.HostAndPort;
import java.util.concurrent.CompletionStage;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.LookupResult;
import com.spotify.folsom.AbstractRawMemcacheClient;
import com.spotify.folsom.ConnectionChangeListener;
import com.spotify.folsom.ObservableClient;
import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.client.NotConnectedClient;
import com.spotify.folsom.client.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SrvKetamaClient extends AbstractRawMemcacheClient {
  private static final Logger log = LoggerFactory.getLogger(SrvKetamaClient.class);
  public static final int MIN_DNS_WAIT_TIME = 10;
  public static final int MAX_DNS_WAIT_TIME = 3600;

  private final ScheduledExecutorService executor;
  private final String srvRecord;
  private final DnsSrvResolver srvResolver;
  private final long period;
  private final TimeUnit periodUnit;

  private final Connector connector;
  private final long shutdownDelay;
  private final TimeUnit shutdownUnit;
  private final MyConnectionChangeListener listener = new MyConnectionChangeListener();

  private ScheduledFuture<?> refreshJob;

  private final Object sync = new Object();

  private volatile List<HostAndPort> addresses = Collections.emptyList();
  private volatile RawMemcacheClient currentClient;
  private volatile RawMemcacheClient pendingClient = null;
  private boolean shutdown = false;


  public SrvKetamaClient(final String srvRecord,
                         DnsSrvResolver srvResolver,
                         ScheduledExecutorService executor,
                         long period, TimeUnit periodUnit,
                         final Connector connector,
                         long shutdownDelay, TimeUnit shutdownUnit) {
    this.srvRecord = srvRecord;
    this.srvResolver = srvResolver;
    this.period = period;
    this.periodUnit = periodUnit;
    this.connector = connector;
    this.shutdownDelay = shutdownDelay;
    this.shutdownUnit = shutdownUnit;
    this.executor = executor;
    this.currentClient = NotConnectedClient.INSTANCE;
    this.currentClient.registerForConnectionChanges(listener);
  }

  public void start() {
    if (refreshJob != null) {
      throw new RuntimeException("You may only start this once");
    }
    refreshJob = this.executor.schedule(this::updateDNS, 0, TimeUnit.MILLISECONDS);
  }

  public void updateDNS() {
    synchronized (sync) {
      if (shutdown) {
        return;
      }
      long ttl = TimeUnit.SECONDS.convert(period, periodUnit);
      try {
        List<LookupResult> lookupResults = srvResolver.resolve(srvRecord);
        List<HostAndPort> hosts = Lists.newArrayListWithCapacity(lookupResults.size());
        for (LookupResult lookupResult : lookupResults) {
          hosts.add(HostAndPort.fromParts(lookupResult.host(), lookupResult.port()));
          ttl = Math.min(ttl, lookupResult.ttl());
        }
        List<HostAndPort> newAddresses = Ordering.from(HostAndPortComparator.INSTANCE)
                .sortedCopy(hosts);
        if (!newAddresses.equals(addresses)) {
          addresses = newAddresses;
          log.info("Connecting to " + newAddresses);
          List<AddressAndClient> addressAndClients = getAddressesAndClients(newAddresses);
          setPendingClient(addressAndClients);
        }
      } finally {
        long delay = clamp(MIN_DNS_WAIT_TIME, MAX_DNS_WAIT_TIME, ttl);
        refreshJob = this.executor.schedule(this::updateDNS, delay, TimeUnit.SECONDS);
      }
    }
  }

  private long clamp(int min, int max, long value) {
    return Math.max(min, Math.min(max, value));
  }

  private List<AddressAndClient> getAddressesAndClients(List<HostAndPort> newAddresses) {
    List<AddressAndClient> res = Lists.newArrayListWithCapacity(newAddresses.size());
    for (HostAndPort address : newAddresses) {
      res.add(new AddressAndClient(address, connector.connect(address)));
    }
    return res;
  }

  @Override
  public <T> CompletionStage<T> send(Request<T> request) {
    return currentClient.send(request);
  }

  @Override
  public void shutdown() {
    if (refreshJob != null) {
      refreshJob.cancel(false);
    }

    final RawMemcacheClient pending;
    synchronized (sync) {
      shutdown = true;
      pending = pendingClient;
      pendingClient = null;
      currentClient.shutdown();
    }
    if (pending != null) {
      pending.unregisterForConnectionChanges(listener);
      pending.shutdown();
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

  public interface Connector {
    RawMemcacheClient connect(HostAndPort input);
  }

  private void setPendingClient(List<AddressAndClient> addressAndClients) {
    final RawMemcacheClient newPending = new KetamaMemcacheClient(addressAndClients);
    newPending.registerForConnectionChanges(listener);
    final RawMemcacheClient oldPending;

    synchronized (sync) {
      oldPending = pendingClient;
      pendingClient = newPending;
    }

    newPending.connectFuture().thenRun(() -> {
      final RawMemcacheClient oldClient;
      synchronized (sync) {
        if (newPending != pendingClient) {
          // We don't care about this event if it's not the expected client
          return;
        }

        oldClient = currentClient;
        currentClient = pendingClient;
        pendingClient = null;
      }
      notifyConnectionChange();
      executor.schedule(new ShutdownJob(oldClient), shutdownDelay, shutdownUnit);
    });
    if (oldPending != null) {
      oldPending.unregisterForConnectionChanges(listener);
      oldPending.shutdown();
    }
  }

  private static class HostAndPortComparator implements Comparator<HostAndPort> {
    private static final HostAndPortComparator INSTANCE = new HostAndPortComparator();

    @Override
    public int compare(HostAndPort o1, HostAndPort o2) {
      int cmp = o1.getHostText().compareTo(o2.getHostText());
      if (cmp != 0) {
        return cmp;
      }
      return Integer.compare(o1.getPort(), o2.getPort());
    }
  }

  private class ShutdownJob implements Runnable {
    private final RawMemcacheClient oldClient;

    public ShutdownJob(RawMemcacheClient oldClient) {
      this.oldClient = oldClient;
    }

    @Override
    public void run() {
      oldClient.unregisterForConnectionChanges(listener);
      oldClient.shutdown();
    }
  }

  private class MyConnectionChangeListener implements ConnectionChangeListener {
    @Override
    public void connectionChanged(ObservableClient client) {
      notifyConnectionChange();
    }
  }
}
