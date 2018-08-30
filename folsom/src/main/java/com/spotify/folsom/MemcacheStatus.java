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

/**
 * Response status codes from
 * https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#response-status
 */
public enum MemcacheStatus {
  OK,
  KEY_NOT_FOUND,
  KEY_EXISTS,
  VALUE_TOO_LARGE,
  INVALID_ARGUMENTS,
  ITEM_NOT_STORED,
  INCR_DECR_ON_NON_NUMERIC_VALUE,
  THE_VBUCKET_BELONGS_TO_ANOTHER_SERVER,
  AUTHENTICATION_ERROR,
  AUTHENTICATION_CONTINUE,
  UNKNOWN_COMMAND,
  OUT_OF_MEMORY,
  NOT_SUPPORTED,
  INTERNAL_ERROR,
  BUSY,
  TEMPORARY_FAILURE,

  // This status code is not defined in the above list, but it is referenced here:
  // https://github.com/memcached/memcached/wiki/SASLAuthProtocol#unauthorized
  UNAUTHORIZED;

  public static MemcacheStatus fromInt(final int status) throws IOException {
    switch (status) {
      case 0x00: return OK;
      case 0x01: return KEY_NOT_FOUND;
      case 0x02: return KEY_EXISTS;
      case 0x03: return VALUE_TOO_LARGE;
      case 0x04: return INVALID_ARGUMENTS;
      case 0x05: return ITEM_NOT_STORED;
      case 0x06: return INCR_DECR_ON_NON_NUMERIC_VALUE;
      case 0x07: return THE_VBUCKET_BELONGS_TO_ANOTHER_SERVER;
      case 0x08: return AUTHENTICATION_ERROR;
      case 0x09: return AUTHENTICATION_CONTINUE;
      case 0x20: return UNAUTHORIZED;
      case 0x81: return UNKNOWN_COMMAND;
      case 0x82: return OUT_OF_MEMORY;
      case 0x83: return NOT_SUPPORTED;
      case 0x84: return INTERNAL_ERROR;
      case 0x85: return BUSY;
      case 0x86: return TEMPORARY_FAILURE;
      default: throw new IOException(String.format("Unknown status code: 0x%2x", status));
    }
  }
}
