package com.spotify.folsom.client.binary;

import com.spotify.folsom.MemcacheStatus;
import com.spotify.folsom.client.AllRequest;
import com.spotify.folsom.client.Request;
import java.util.List;

public class DeleteAllRequest extends DeleteRequest implements AllRequest<MemcacheStatus> {
  public DeleteAllRequest(byte[] key) {
    super(key, 0L);
  }

  @Override
  public MemcacheStatus merge(List<MemcacheStatus> results) {
    return AllRequest.mergeMemcacheStatus(results);
  }

  @Override
  public Request<MemcacheStatus> duplicate() {
    return new DeleteAllRequest(key);
  }
}
