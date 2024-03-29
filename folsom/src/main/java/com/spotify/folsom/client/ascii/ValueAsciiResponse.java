/*
 * Copyright (c) 2014-2015 Spotify AB
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

import java.util.ArrayList;
import java.util.List;

public class ValueAsciiResponse extends AsciiResponse {

  public final List<ValueResponse> values = new ArrayList<>(50);

  public ValueAsciiResponse() {
    super(Type.VALUE);
  }

  public void addGetResult(byte[] key, byte[] value, long cas, int flags) {
    values.add(new ValueResponse(key, value, cas, flags));
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }
}
