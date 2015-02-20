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

import com.google.common.base.Charsets;
import com.spotify.folsom.client.Request;

import java.io.IOException;
import java.nio.charset.Charset;

public abstract class AsciiRequest<T> extends Request<T> {
  protected static final byte[] NEWLINE_BYTES = "\r\n".getBytes(Charsets.US_ASCII);
  protected static final byte SPACE_BYTES = ' ';

  protected AsciiRequest(String key, Charset charset) {
    super(key, charset);
  }

  protected AsciiRequest(byte[] key) {
    super(key);
  }

  @Override
  public void handle(Object response) throws IOException {
    handle((AsciiResponse) response);
  }

  protected abstract void handle(AsciiResponse response) throws IOException;
}
