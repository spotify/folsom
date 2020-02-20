/*
 * Copyright (c) 2014-2019 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.folsom.elasticache;

import com.spotify.folsom.guava.HostAndPort;
import java.util.List;

public class Response {

  private final int configurationVersion;
  private final List<HostAndPort> hosts;

  public Response(final int configurationVersion, final List<HostAndPort> result) {
    this.configurationVersion = configurationVersion;
    this.hosts = result;
  }

  public int getConfigurationVersion() {
    return configurationVersion;
  }

  public List<HostAndPort> getHosts() {
    return hosts;
  }
}
