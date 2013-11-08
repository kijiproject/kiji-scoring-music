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
import java.util.Map;

import com.google.common.collect.Maps;

import org.kiji.examples.music.TopSongs;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiURI;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerGetStoresContext;
import org.kiji.scoring.ScoreFunction;

/**
 * ScoreFunction implementation which recommends songs based on a user's most recently played track
 * and precalculated track-track recommendations.
 */
public class NextSongRecommenderScoreFunction extends ScoreFunction<String> {

  /** Parameter key which holds the name of the recommender KeyValueStore. */
  public static final String KEY_VALUE_STORE_NAME_PARAMETER_KEY =
      "org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_name";
  /** Parameter key which holds the KijiURI of the table from which recommendations are read. */
  public static final String KEY_VALUE_STORE_TABLE_URI_PARAMETER_KEY =
      "org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_table_uri";
  /** Parameter key which hold the name of the column from which recommendations are read. */
  public static final String KEY_VALUE_STORE_COLUMN_PARAMETER_KEY =
      "org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_column";

  // One time setup methods ------------------------------------------------------------------------

  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores(
      final FreshenerGetStoresContext context
  ) {
    final String kvStoreName = context.getParameter(KEY_VALUE_STORE_NAME_PARAMETER_KEY);
    final String tableURI = context.getParameter(KEY_VALUE_STORE_TABLE_URI_PARAMETER_KEY);
    final String column = context.getParameter(KEY_VALUE_STORE_COLUMN_PARAMETER_KEY);

    final KeyValueStore<?, ?> kvstore = KijiTableKeyValueStore.builder()
        .withTable(KijiURI.newBuilder(tableURI).build())
        .withColumn(new KijiColumnName(column))
        .build();

    final Map<String, KeyValueStore<?, ?>> stores = Maps.newHashMap();
    stores.put(kvStoreName, kvstore);
    return stores;
  }

  // Per-request methods ---------------------------------------------------------------------------

  @Override
  public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
    // Get just the user's most recent track play.
    return KijiDataRequest.create("info", "track_plays");
  }

  @Override
  public TimestampedValue<String> score(
      final KijiRowData dataToScore, final FreshenerContext context
  ) throws IOException {
    // Open the key value store reader from which we can get recommendations.
    final KeyValueStoreReader<KijiRowKeyComponents, TopSongs> topNextSongsReader =
        context.getStore(context.getParameter(KEY_VALUE_STORE_NAME_PARAMETER_KEY));
    // Get the most recent track played by the user.
    final String mostRecentSong = dataToScore.getMostRecentValue("info", "track_plays").toString();
    // Return the recommended song from the top songs.
    return TimestampedValue.create(recommend(mostRecentSong, topNextSongsReader));
  }

  /**
   * Recommend the most popular song played after the song most recently played by the user.
   *
   * <p>
   *   This method is package private for testing purposes.
   * </p>
   *
   * @param mostRecentSong Song played most recently by the user.
   * @param recommendationReader KeyValueStoreReader from which to get song recommendations.
   * @return the song ID of the recommended song.
   * @throws IOException in case of an error reading from the key value store.
   */
  static String recommend(
      final String mostRecentSong,
      final KeyValueStoreReader<KijiRowKeyComponents, TopSongs> recommendationReader
  ) throws IOException {
    // From the recommendation key value store.
    return recommendationReader
        // Get the list of top songs played after mostRecentSong.
        .get(KijiRowKeyComponents.fromComponents(mostRecentSong))
        // Extract the list from the Avro record.
        .getTopSongs()
        // Get the most popular song.
        .get(0)
        // Get the song ID as a String and return it.
        .getSongId().toString();
  }
}
