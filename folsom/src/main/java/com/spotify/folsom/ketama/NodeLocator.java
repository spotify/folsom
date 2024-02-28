package com.spotify.folsom.ketama;

import com.spotify.folsom.RawMemcacheClient;

public interface NodeLocator {

  RawMemcacheClient findClient(final byte[] key);
}
