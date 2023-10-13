/*
 * Copyright (c) 2023 Spotify AB
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
package com.spotify.folsom.reconnect;

import org.junit.Test;

public class ReconnectionListenerTest {
  @Test
  public void ensureEmptyClassCompiles() {
    // This class acts as a test. Nothing should be necessary from the abstract class, as such this
    // one line of code should compile at all times.
    //
    // Needing to change this line should result in a minor version update, at least.
    @SuppressWarnings("unused")
    class EmptyReconnectionListener extends AbstractReconnectionListener {}
  }
}
