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

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.KijiFreshnessPolicy;

/**
 * A KijiFreshnessPolicy implementation designed to be paired with
 * {@link NextSongRecommenderScoreFunction}. Recommendation data is considered fresh if it has been
 * updated since the user last played a new track.
 */
public class NextSongRecommenderFreshnessPolicy extends KijiFreshnessPolicy {

  /** Column storing the user's track play history. */
  private static final KijiColumnName PLAYED = new KijiColumnName("info", "track_plays");
  /** Column storing our history of recommendations for the user. */
  private static final KijiColumnName RECOMMENDED = new KijiColumnName("info", "next_song_rec");
  /** Data request containing the above columns which are necessary for determining freshness. */
  private static final KijiDataRequest REQUEST = KijiDataRequest.builder()
      .addColumns(KijiDataRequestBuilder.ColumnsDef.create().withMaxVersions(1).add(PLAYED))
      .addColumns(KijiDataRequestBuilder.ColumnsDef.create().withMaxVersions(1).add(RECOMMENDED))
      .build();

  /** {@inheritDoc} */
  @Override
  public boolean shouldUseClientDataRequest(
      final FreshenerContext context
  ) {
    final KijiDataRequest clientRequest = context.getClientRequest();
    // If the clientRequest contains both PLAYED and RECOMMENDED then it fulfills our needs,
    // otherwise, we should use our custom request.
    return null != clientRequest.getColumn(PLAYED) && null != clientRequest.getColumn(RECOMMENDED);
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest(
      final FreshenerContext context
  ) {
    // If the clientRequest did not contain both of the columns we need, we can request them here.
    return REQUEST;

  }

  /** {@inheritDoc} */
  @Override
  public boolean isFresh(
      final KijiRowData kijiRowData,
      final FreshenerContext context
  ) {
    final Long lastPlayedTimestamp =
        kijiRowData.getTimestamps(PLAYED.getFamily(), PLAYED.getQualifier()).pollLast();
    final Long lastRecommendedTimestamp =
        kijiRowData.getTimestamps(RECOMMENDED.getFamily(), RECOMMENDED.getQualifier()).pollLast();
    if (null == lastPlayedTimestamp) {
      // If there are no track plays we cannot recommend anything, so that data is fresh.
      return true;
    } else if (null == lastRecommendedTimestamp) {
      // If there are no recommendations, they cannot be fresh.
      return false;
    } else {
      // If we have made a recommendation since the last time the user played a track, that
      // recommendation is still relevant so the data is fresh. Otherwise it is stale.
      return lastPlayedTimestamp < lastRecommendedTimestamp;
    }
  }
}
