package com.spotify.folsom.client.binary;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.AllRequest;
import java.util.List;

public class DeleteAllRequest extends DeleteRequest implements AllRequest<MemcacheStatus> {
  public DeleteAllRequest(byte[] key) {
    super(key);
  }

  @Override
  public MemcacheStatus merge(List<MemcacheStatus> results) {
    return AllRequest.mergeMemcacheStatus(results);
  }
}
