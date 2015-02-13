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

package com.spotify.folsom;

import java.io.IOException;

public enum MemcacheStatus {
  OK,
  KEY_NOT_FOUND,
  KEY_EXISTS,
  VALUE_TOO_LARGE,
  INVALID_ARGUMENTS,
  ITEM_NOT_STORED;

  public static MemcacheStatus fromInt(final int status) throws IOException {
    switch (status) {
      case 0: return OK;
      case 1: return KEY_NOT_FOUND;
      case 2: return KEY_EXISTS;
      case 3: return VALUE_TOO_LARGE;
      case 4: return INVALID_ARGUMENTS;
      case 5: return ITEM_NOT_STORED;
      default: throw new IOException(String.format("Unknown status code: 0x%2x", status));
    }
  }
}
