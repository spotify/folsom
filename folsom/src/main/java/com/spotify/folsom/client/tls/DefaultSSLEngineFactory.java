package com.spotify.folsom.client.tls;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.security.NoSuchAlgorithmException;

public class DefaultSSLEngineFactory implements SSLEngineFactory {
  final SSLContext sslContext;
  final boolean reuseSession;

  public DefaultSSLEngineFactory(final boolean reuseSession) throws NoSuchAlgorithmException {
    this(SSLContext.getDefault(), reuseSession);
  }

  public DefaultSSLEngineFactory(final SSLContext sslContext, final boolean reuseSession) {
    this.sslContext = sslContext;
    this.reuseSession = reuseSession;
  }

  @Override
  public SSLEngine createSSLEngine(String hostname, int port) {
    final SSLEngine sslEngine;

    if(reuseSession) {
      sslEngine = sslContext.createSSLEngine(hostname, port);
    } else {
      sslEngine = sslContext.createSSLEngine();
    }

    sslEngine.setUseClientMode(true);
    return sslEngine;
  }
}
