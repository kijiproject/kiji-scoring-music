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

import java.util.List;

import com.google.common.base.Preconditions;

import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;
import org.kiji.scoring.FreshKijiTableReader;

/** CLI for writing track plays and reading recommendations. */
public class ScoringMusicTool extends BaseTool {

  @Flag(name="kiji", usage="the KijiURI of the kiji music tutorial instance.")
  private String mURIFlag = null;

  @Flag(name="write-user", usage="specify the user for which to generate a new random track play. "
      + "user names are of the format 'user-#' where # is a value between 0 and 49 inclusive with "
      + "no leading zero.")
  private String mWriteUserFlag = null;

  @Flag(name="write-all", usage="Generate a new random track play for all users.")
  private Boolean mWriteAllFlag = null;

  @Flag(name="freshen-user", usage="specify the user for which to perform a freshened read request."
      + " User names are of the format 'user-#' where # is a value between 0 and 49 inclusive with "
      + "no leading zero. If specified with a writing flag, this command will be executed last.")
  private String mFreshenUserFlag = null;

  /** The KijiURI built from the mURIFlag. */
  private KijiURI mURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "scoring-music";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Generate track plays and perform fresh reads for users in the KijiMusic example.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Data";
  }

  /**
   * Validate a username. Checks that the name begins with "user-" and that any character after
   * "user-" are a valid number between 0 and 49 inclusive.
   *
   * @param userFlag the user specified name to validate.
   * @return whether the specified name is a valid username.
   */
  private static boolean isValidUser(
      final String userFlag
  ) {
    if (userFlag.startsWith("user-")) {
      try {
        final int userId = Integer.valueOf(userFlag.substring(5));
        // Check that the id is within the legal range.
        return userId >= 0 && userId < 50;
      } catch (NumberFormatException nfe) {
        // Not a valid number means not a value user.
        return false;
      }
    } else {
      // Name does not start with "user-" so it is not a valid username.
      return false;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() {
    Preconditions.checkArgument(null != mURIFlag && !mURIFlag.isEmpty(),
        "Please specify the KijiURI of the music tutorial instance. If running the tutorial in the "
        + "BentoBox, this URI should be in the environment variable $KIJI");
    mURI = KijiURI.newBuilder(mURIFlag).build();
    Preconditions.checkArgument(
        (null != mWriteAllFlag && mWriteAllFlag)
        || (null != mWriteUserFlag && !mWriteUserFlag.isEmpty())
        || (null != mFreshenUserFlag && !mFreshenUserFlag.isEmpty()),
        "Please specify at least one of --write-all, --write-user, or --freshen-user.");
    if (null != mWriteUserFlag && !mWriteUserFlag.isEmpty()) {
      Preconditions.checkArgument(isValidUser(mWriteUserFlag), String.format(
          "Specified user: '%s' is invalid. A user name must follow the format 'user-#' where # is "
          + "a value between 0 and 49 inclusive with no leading zero.", mWriteUserFlag));
    }
    if (null != mFreshenUserFlag && !mFreshenUserFlag.isEmpty()) {
      Preconditions.checkArgument(isValidUser(mFreshenUserFlag), String.format(
          "Specified user: '%s' is invalid. A user name must follow the format 'user-#' where # is "
              + "a value between 0 and 49 inclusive with no leading zero.", mFreshenUserFlag));
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(final List<String> nonFlagArgs) throws Exception {
    final TrackPlayGenerator generator = new TrackPlayGenerator(mURI, 10);
    if ((null != mWriteAllFlag) && mWriteAllFlag) {
      generator.generateRandomTrackPlayForAll();
    }
    if (null != mWriteUserFlag) {
      generator.generateRandomTrackPlayForUser(mWriteUserFlag);
    }
    if (null != mFreshenUserFlag) {
      final Kiji kiji = Kiji.Factory.open(mURI);
      try {
        final KijiTable table = kiji.openTable("users");
        try {
          final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
              .withTable(table)
              .withTimeout(1000)
              .build();
          try {
            final EntityId eid = table.getEntityId(mFreshenUserFlag);
            final KijiDataRequest request = KijiDataRequest.create("info", "next_song_rec");
            final String freshRecommendation = freshReader
                .get(eid, request)
                .getMostRecentValue("info", "next_song_rec")
                .toString();
            getPrintStream().println(String.format(
                "Fresh recommendation for user '%s' is '%s'",
                mFreshenUserFlag, freshRecommendation));
          } finally {
            freshReader.close();
          }
        } finally {
          table.release();
        }
      } finally {
        kiji.release();
      }
    }

    return BaseTool.SUCCESS;
  }
}
