package com.spotify.folsom.client.tls;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.security.NoSuchAlgorithmException;

public class DefaultSSLEngineFactory implements SSLEngineFactory {
  final SSLContext sslContext;

  public DefaultSSLEngineFactory() throws NoSuchAlgorithmException {
    this(SSLContext.getDefault());
  }

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
