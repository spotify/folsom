package com.spotify.folsom;

import com.google.common.base.Suppliers;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class KetamaServers {

  public static final Supplier<KetamaServers> SIMPLE_INSTANCE =
      Suppliers.memoize(() -> new KetamaServers(3))::get;

  private final List<MemcachedServer> servers;

  public KetamaServers(int instances) {
    servers = new ArrayList<>();
    for (int i = 0; i < instances; i++) {
      servers.add(new MemcachedServer());
    }
  }

  public void setup() {
    for (MemcachedServer server : servers) {
      server.start();
    }
  }

  public void stop() {
    for (MemcachedServer server : servers) {
      server.stop();
    }
  }

  public MemcachedServer getInstance(int i) {
    return servers.get(i);
  }

  List<MemcachedServer> getServers() {
    return servers;
  }

  void flush() {
    servers.forEach(MemcachedServer::flush);
  }
}
