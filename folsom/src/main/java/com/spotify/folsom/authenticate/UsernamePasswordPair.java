package com.spotify.folsom.authenticate;

import static java.util.Objects.requireNonNull;

public class UsernamePasswordPair {
  private final String username;
  private final String password;

  public UsernamePasswordPair(final String username, final String password) {
    this.username = requireNonNull(username);
    this.password = requireNonNull(password);
  }

  public AsciiAuthenticator getAsciiAuthenticator() {
    return new AsciiAuthenticator(username, password);
  }

  public PlaintextAuthenticator getPlainTextAuthenticator() {
    return new PlaintextAuthenticator(username, password);
  }
}
