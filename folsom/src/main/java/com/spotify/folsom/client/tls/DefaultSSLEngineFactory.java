package com.spotify.folsom.client.tls;

import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

public class DefaultSSLEngineFactory implements SSLEngineFactory {
  private final SSLContext sslContext;
  private final boolean reuseSession;

  public DefaultSSLEngineFactory(final boolean reuseSession) throws NoSuchAlgorithmException {
    this(SSLContext.getDefault(), reuseSession);
  }

  public DefaultSSLEngineFactory(final SSLContext sslContext, final boolean reuseSession) {
    this.sslContext = sslContext;
    this.reuseSession = reuseSession;
  }

  @Override
  public SSLEngine createSSLEngine(final String hostname, final int port) {
    final SSLEngine sslEngine;

    if (reuseSession) {
      sslEngine = sslContext.createSSLEngine(hostname, port);
    } else {
      sslEngine = sslContext.createSSLEngine();
    }

    sslEngine.setUseClientMode(true);
    return sslEngine;
  }
}
