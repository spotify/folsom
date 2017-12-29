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

package com.spotify.folsom.transcoder;

import com.google.common.base.Charsets;
import com.spotify.folsom.Transcoder;

import java.nio.charset.Charset;


public class StringTranscoder implements Transcoder<String> {

  public static final StringTranscoder UTF8_INSTANCE = new StringTranscoder(Charsets.UTF_8);

  private final Charset charset;

  public StringTranscoder(final Charset charset) {
    this.charset = charset;
  }

  @Override
  public byte[] encode(final String t) {
    return t.getBytes(charset);
  }

  @Override
  public String decode(final byte[] b) {
    return new String(b, charset);
  }

}
