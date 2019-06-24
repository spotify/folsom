/*
 * Copyright (c) 2019 Spotify AB
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

package com.spotify.folsom.client.ascii;

import java.util.HashMap;
import java.util.Map;

public class StatsAsciiResponse extends AsciiResponse {

  public final Map<String, String> values = new HashMap<>();

  public StatsAsciiResponse() {
    super(Type.STATS);
  }

  public void addStat(String name, String value) {
    values.put(name, value);
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }
}
