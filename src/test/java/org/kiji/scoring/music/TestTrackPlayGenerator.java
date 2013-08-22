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

import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

public class TestTrackPlayGenerator extends KijiClientTest {

  private KijiTable mTable;

  @Before
  public void setupTestTrackPlayGenerator() throws IOException {
    // Get the test table layouts.
    final KijiTableLayout layout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");

    // Populate the environment.
    new InstanceBuilder(getKiji())
        .withTable("users", layout)
        .build();

    // Fill local variables.
    mTable = getKiji().openTable("users");
  }

  @After
  public void cleanupTestTrackPlayGenerator() throws IOException {
    mTable.release();
  }

  @Test
  public void testTrackPlayGenerator() throws IOException, InterruptedException {
    final KijiURI tableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName("users").build();
    final TrackPlayGenerator generator = new TrackPlayGenerator(tableURI, 10);
    generator.start();
    Thread.sleep(1000);
    generator.stopGenerating();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiDataRequest request = KijiDataRequest.create("info", "track_plays");

      final Map<String, String> generated = generator.getGeneratedPlays();
      for (Map.Entry<String, String> entry : generated.entrySet()) {
        final EntityId userEID = mTable.getEntityId(entry.getKey());
        assertEquals(entry.getValue(),
            reader.get(userEID, request).getMostRecentValue("info", "track_plays").toString());
      }
    } finally {
      reader.close();
    }
  }
}
