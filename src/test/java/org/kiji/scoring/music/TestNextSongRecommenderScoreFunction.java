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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.kiji.examples.music.SongCount;
import org.kiji.examples.music.TopSongs;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.InMemoryMapKeyValueStore;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.lib.AlwaysFreshen;

/** Tests for {@link org.kiji.scoring.music.NextSongRecommenderScoreFunction}. */
public final class TestNextSongRecommenderScoreFunction extends KijiClientTest {

  private static final String SONG_1 = "song-1";
  private static final String SONG_2 = "song-2";
  private static final KijiRowKeyComponents KEY1 = KijiRowKeyComponents.fromComponents(SONG_1);
  private static final KijiRowKeyComponents KEY2 = KijiRowKeyComponents.fromComponents(SONG_2);
  private static final SongCount SONGCOUNT1 =
      SongCount.newBuilder().setSongId(SONG_1).setCount(5).build();
  private static final SongCount SONGCOUNT2 =
      SongCount.newBuilder().setSongId(SONG_2).setCount(10).build();
  private static final List<SongCount> SONGS1 = Lists.newArrayList(SONGCOUNT1);
  private static final List<SongCount> SONGS2 = Lists.newArrayList(SONGCOUNT2);
  private static final TopSongs TOPSONGS1 = TopSongs.newBuilder().setTopSongs(SONGS1).build();
  private static final TopSongs TOPSONGS2 = TopSongs.newBuilder().setTopSongs(SONGS2).build();

  private static final String USER_1 = "user-1";
  private static final String USER_2 = "user-2";

  private static final String USERS_LAYOUT_PATH = "layout/users.json";
  private static final String SONGS_LAYOUT_PATH = "layout/songs.json";

  private static final KijiColumnName TRACK_PLAYS = new KijiColumnName("info", "track_plays");
  private static final KijiColumnName TOP_NEXT_SONGS = new KijiColumnName("info", "top_next_songs");
  private static final KijiColumnName NEXT_SONG_REC = new KijiColumnName("info", "next_song_rec");

  /** Create a dummy of the KVStore used for getting recommendations for testing. */
  private InMemoryMapKeyValueStore<KijiRowKeyComponents, TopSongs> getDummyKVStore() {
    return InMemoryMapKeyValueStore.fromMap(ImmutableMap.of(KEY1, TOPSONGS2, KEY2, TOPSONGS1));
  }

  @Test
  public void testRecommend() throws IOException {
    final InMemoryMapKeyValueStore<KijiRowKeyComponents, TopSongs> dummyKVStore =
        getDummyKVStore();
    final KeyValueStoreReader<KijiRowKeyComponents, TopSongs> reader = dummyKVStore.open();
    try {
      final String recommendedForSong1 = NextSongRecommenderScoreFunction.recommend(SONG_1, reader);
      final String recommendedForSong2 = NextSongRecommenderScoreFunction.recommend(SONG_2, reader);

      Assert.assertEquals(SONG_1, recommendedForSong2);
      Assert.assertEquals(SONG_2, recommendedForSong1);
    } finally {
      reader.close();
    }
  }

  private void populateTables() throws IOException {
    new InstanceBuilder(getKiji())
        // Populate the "users" table.
        .withTable(KijiTableLayouts.getLayout(USERS_LAYOUT_PATH))
            .withRow(USER_1)
                .withFamily(TRACK_PLAYS.getFamily())
                    .withQualifier(TRACK_PLAYS.getQualifier())
                        .withValue(SONG_1)
            .withRow(USER_2)
                .withFamily(TRACK_PLAYS.getFamily())
                    .withQualifier(TRACK_PLAYS.getQualifier())
                        .withValue(SONG_2)
        // Populate the "songs" table.
        .withTable(KijiTableLayouts.getLayout(SONGS_LAYOUT_PATH))
            .withRow(SONG_1)
                .withFamily(TOP_NEXT_SONGS.getFamily())
                    .withQualifier(TOP_NEXT_SONGS.getQualifier())
                        .withValue(TOPSONGS2)
            .withRow(SONG_2)
                .withFamily(TOP_NEXT_SONGS.getFamily())
                     .withQualifier(TOP_NEXT_SONGS.getQualifier())
                        .withValue(TOPSONGS1)
        .build();
  }

  @Test
  public void testEndToEnd() throws IOException {
    // Build minimal user and song tables.
    populateTables();

    // Register NextSongRecommenderScoreFunction in a Freshener with an AlwaysFreshen policy and the
    // necessary configuration parameters.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      final Map<String, String> parameters = ImmutableMap.of(
          NextSongRecommenderScoreFunction.KEY_VALUE_STORE_NAME_PARAMETER_KEY,
              "nextPlayed",
          NextSongRecommenderScoreFunction.KEY_VALUE_STORE_COLUMN_PARAMETER_KEY,
              "info:top_next_songs",
          NextSongRecommenderScoreFunction.KEY_VALUE_STORE_TABLE_URI_PARAMETER_KEY,
              KijiURI.newBuilder(getKiji().getURI()).withTableName("songs").build().toString());
      manager.registerFreshener(
          "users",
          NEXT_SONG_REC,
          new AlwaysFreshen(),
          new NextSongRecommenderScoreFunction(),
          parameters,
          false,
          false);
    } finally {
      manager.close();
    }

    // Open the users table and perform some freshened recommendation requests.
    final KijiTable userTable = getKiji().openTable("users");
    try {
      final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
          .withTable(userTable)
          .withTimeout(500)
          .build();

      final KijiDataRequest recommendationRequest = KijiDataRequest.create("info", "next_song_rec");

      final KijiRowData user1Data =
          freshReader.get(userTable.getEntityId(USER_1), recommendationRequest);
      final KijiRowData user2Data =
          freshReader.get(userTable.getEntityId(USER_2), recommendationRequest);
      // Ensure that we recommend song-1 to user-2 and song-2 to user-1.
      Assert.assertEquals(SONG_1,
          user2Data.getMostRecentValue("info", "next_song_rec").toString());
      Assert.assertEquals(SONG_2,
          user1Data.getMostRecentValue("info", "next_song_rec").toString());
    } finally {
      userTable.release();
    }
  }
}
