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

package com.spotify.folsom.client.binary;


import com.spotify.folsom.MemcacheStatus;

public class ResponsePacket {
  public final byte opcode;
  public final MemcacheStatus status;
  public final int opaque;
  public final long cas;
  public final byte[] value;

  public ResponsePacket(final byte opcode,
                        final MemcacheStatus status,
                        final int opaque,
                        final long cas,
                        final byte[] value) {
    this.opcode = opcode;
    this.status = status;
    this.opaque = opaque;
    this.cas = cas;
    this.value = value;
  }
}
