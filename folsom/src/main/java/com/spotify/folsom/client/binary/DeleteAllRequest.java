package com.spotify.folsom.client.binary;

import com.spotify.folsom.client.AllRequest;

public class DeleteAllRequest extends DeleteRequest implements AllRequest {
  public DeleteAllRequest(byte[] key) {
    super(key);
  }
}
