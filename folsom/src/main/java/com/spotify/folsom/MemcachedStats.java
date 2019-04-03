package com.spotify.folsom;

import java.util.Map;

public class MemcachedStats {
  private final Map<String, String> stats;

  public MemcachedStats(Map<String, String> stats) {
    this.stats = stats;
  }

  public Map<String, String> getStats() {
    return stats;
  }
}
