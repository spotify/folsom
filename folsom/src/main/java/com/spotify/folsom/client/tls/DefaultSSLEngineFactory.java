package com.spotify.folsom.client.tls;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

public class DefaultSSLEngineFactory implements SSLEngineFactory {
  final SSLContext sslContext;

  public DefaultSSLEngineFactory(SSLContext sslContext) {
    this.sslContext = sslContext;
  }

  @Override
  public SSLEngine createSSLEngine(String hostname, int port) {
    SSLEngine sslEngine = sslContext.createSSLEngine(hostname, port);
    sslEngine.setUseClientMode(true);
    return sslEngine;
  }
}
