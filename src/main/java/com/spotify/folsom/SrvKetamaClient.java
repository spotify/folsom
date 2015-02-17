package com.spotify.folsom;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.folsom.client.NotConnectedClient;
import com.spotify.folsom.client.Request;
import com.spotify.folsom.ketama.AddressAndClient;
import com.spotify.folsom.ketama.KetamaMemcacheClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SrvKetamaClient extends AbstractRawMemcacheClient {
  private static final Logger log = LoggerFactory.getLogger(SrvKetamaClient.class);

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
    refreshJob = this.executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        updateDNS();
      }
    }, 0, period, periodUnit);
  }

  public void updateDNS() {
    synchronized (sync) {
      if (shutdown) {
        return;
      }
      List<HostAndPort> newAddresses = Ordering.from(HostAndPortComparator.INSTANCE)
              .sortedCopy(srvResolver.resolve(srvRecord));
      if (!newAddresses.equals(addresses)) {
        addresses = newAddresses;
        log.info("Connecting to " + newAddresses);
        List<AddressAndClient> addressAndClients = getAddressesAndClients(newAddresses);
        setPendingClient(addressAndClients);
      }
    }
  }

  private List<AddressAndClient> getAddressesAndClients(List<HostAndPort> newAddresses) {
    List<AddressAndClient> res = Lists.newArrayListWithCapacity(newAddresses.size());
    for (HostAndPort address : newAddresses) {
      res.add(new AddressAndClient(address, connector.connect(address)));
    }
    return res;
  }

  @Override
  public <T> ListenableFuture<T> send(Request<T> request) {
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

    ListenableFuture<Void> future = ConnectFuture.connectFuture(newPending);
    Futures.addCallback(future, new FutureCallback<Void>() {
      @Override
      public void onSuccess(@Nullable Void result) {
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
      }

      @Override
      public void onFailure(Throwable t) {
        throw new RuntimeException("Programmer bug - this is unreachable code");
      }
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
    public void connectionChanged(RawMemcacheClient client) {
      notifyConnectionChange();
    }
  }
}
