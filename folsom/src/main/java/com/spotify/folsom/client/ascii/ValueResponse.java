/*
 * Copyright (c) 2015 Spotify AB
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

public class ValueResponse {
  public final byte[] key;
  public final byte[] value;
  public final long cas;
  public final int flags;

  public ValueResponse(byte[] key, byte[] value, long cas, int flags) {
    this.cas = cas;
    this.value = value;
    this.key = key;
    this.flags = flags;
  }
}
