package com.spotify.folsom.client.ascii;

import java.io.IOException;

public class AsciiResponseException extends IOException {
  public AsciiResponseException(final String message) {
    super(message);
  }
}
