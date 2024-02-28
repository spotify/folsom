package com.spotify.folsom.client.tls;

import javax.net.ssl.SSLEngine;

public interface SSLEngineFactory {
  SSLEngine createSSLEngine(String hostname, int port);
}
