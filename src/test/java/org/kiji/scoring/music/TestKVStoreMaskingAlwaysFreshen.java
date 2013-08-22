/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.scoring.music;

import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import org.junit.Test;

import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.music.KVStoreMaskingAlwaysFreshen.KijiTableKVStoreConfig;

public final class TestKVStoreMaskingAlwaysFreshen {
  @Test
  public void testSerializeDeserialize() {
    final Gson gson = new Gson();

    // Build the state for the freshness policy.
    final KijiTableKVStoreConfig kvStoreConfig = new KijiTableKVStoreConfig(
        "nextPlayed", "kiji://.env/kiji_music/songs", "info:top_next_songs");
    // Serialize is to a string.
    final String json = gson.toJson(kvStoreConfig);

    final KijiFreshnessPolicy policy = new KVStoreMaskingAlwaysFreshen();
    // Load the policy with the prepared state.
    policy.deserialize(json);
    // Ensure that it is returned intact.
    assertEquals(json, policy.serialize());
  }
}
