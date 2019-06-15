package com.spotify.folsom.ketama;

import com.google.common.collect.Lists;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.LookupResult;
import com.spotify.folsom.Resolver;
import java.util.List;

public class SrvResolver implements Resolver {

  private final DnsSrvResolver srvResolver;
  private final String srvRecord;

  public SrvResolver(DnsSrvResolver srvResolver, String srvRecord) {
    this.srvResolver = srvResolver;
    this.srvRecord = srvRecord;
  }

  @Override
  public List<ResolveResult> resolve() {
    List<LookupResult> lookupResults = srvResolver.resolve(srvRecord);
    List<ResolveResult> results = Lists.newArrayListWithCapacity(lookupResults.size());
    for (LookupResult lookupResult : lookupResults) {
      results.add(new ResolveResult(lookupResult.host(), lookupResult.port(), lookupResult.ttl()));
    }
    return results;
  }
}
