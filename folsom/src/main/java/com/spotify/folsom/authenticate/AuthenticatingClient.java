/*
 * Copyright (c) 2018 Spotify AB
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

package com.spotify.folsom.authenticate;

import com.spotify.folsom.RawMemcacheClient;
import com.spotify.folsom.reconnect.Connector;
import java.util.concurrent.CompletionStage;

public class AuthenticatingClient {

  public static CompletionStage<RawMemcacheClient> authenticate(
      Connector connector,
      final boolean binary,
      final Authenticator authenticator) {

    authenticator.validate(binary);

    CompletionStage<RawMemcacheClient> client = connector.connect();

    return authenticator.authenticate(client);
  }
}
