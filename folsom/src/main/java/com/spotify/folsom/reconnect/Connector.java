package com.spotify.folsom.reconnect;

import com.spotify.folsom.RawMemcacheClient;
import java.util.concurrent.CompletionStage;

public interface Connector {
  CompletionStage<RawMemcacheClient> connect();
}
