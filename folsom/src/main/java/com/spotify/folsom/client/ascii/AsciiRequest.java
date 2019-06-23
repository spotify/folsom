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

import com.spotify.folsom.client.AbstractRequest;
import com.spotify.folsom.guava.HostAndPort;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class AsciiRequest<T> extends AbstractRequest<T> {
  protected static final byte[] NEWLINE_BYTES = "\r\n".getBytes(StandardCharsets.US_ASCII);
  protected static final byte SPACE_BYTES = ' ';

  protected AsciiRequest(byte[] key) {
    super(key);
  }

  @Override
  public void handle(final Object response, final HostAndPort server) throws IOException {
    handle((AsciiResponse) response, server);
  }

  protected abstract void handle(AsciiResponse response, HostAndPort server) throws IOException;
}
