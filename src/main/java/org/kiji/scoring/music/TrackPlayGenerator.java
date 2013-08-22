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
import java.util.Random;

import com.google.common.collect.Maps;
import org.mortbay.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;

/** Generates new track plays for users in real time. */
public final class TrackPlayGenerator extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(TrackPlayGenerator.class);

  /**
   * A Random instance used to generate users and songs. This Random instance is shared by all
   * instances of TrackPlayGenerator.
   */
  private static final Random RANDOM = new Random();

  /** The KijiTableWriter which will perform track play writes. */
  private final KijiTableWriter mWriter;

  /** The KijiTable to which track plays will be written. */
  private final KijiTable mTable;

  /** The time in milliseconds to wait between generating new track plays in {@link #run()}. */
  private final int mDelay;

  /** Most recently generated songs for each user. */
  private final Map<String, String> mGenerated = Maps.newHashMap();

  /** A flag used to stop execution of the {@link #run()} method. */
  private boolean mShouldStop = false;

  /**
   * Constructs a new TrackPlayGenerator which writes to a specified user table.
   *
   * @param tableURI the KijiURI of the user table into which this TrackPlayGenerator will write.
   * @param delay the time in milliseconds between writing each new track play.
   * @throws IOException in case of an error opening the Kiji instance
   */
  public TrackPlayGenerator(final KijiURI tableURI, final int delay) throws IOException {
    mDelay = delay;

    final Kiji kiji = Kiji.Factory.open(tableURI);
    try {
      mTable = kiji.openTable(tableURI.getTable());
      mWriter = mTable.openTableWriter();
    } finally {
      kiji.release();
    }
  }

  /**
   * Record a new track play of the given song for the given user.
   *
   * @param user the user who played the specified track.
   * @param song the track played by the user.
   * @throws IOException in case of an error writing to the table.
   */
  public void recordTrackPlay(final String user, final String song) throws IOException {
    mWriter.put(mTable.getEntityId(user), "info", "track_plays", song);
    mGenerated.put(user, song);
  }

  /**
   * Format a new user name from a given integer id.  Ids may be between 0 and 49 inclusive.
   *
   * @param id the id of the user.
   * @return the name of the user.
   */
  private static String getUserForId(final int id) {
    return String.format("user-%d", id);
  }

  /**
   * Format a new song name from a given integer id.  Ids may be between 0 and 49 inclusive.
   *
   * @param id the id of the song.
   * @return the name of the song.
   */
  private static String getSongForId(final int id) {
    return String.format("song-%d", id);
  }

  /**
   * Get a randomly selected user from our user list.
   *
   * @return a randomly selected user from our user list.
   */
  private static String getRandomUser() {
    return getUserForId(RANDOM.nextInt(50));
  }

  /**
   * Get a randomly selected song from our library.
   *
   * @return a randomly selected song from our library.
   */
  private static String getRandomSong() {
    return getSongForId(RANDOM.nextInt(50));
  }

  /**
   * Create and record a random track play for a random user.
   *
   * @throws IOException in case of an error writing to the table.
   */
  public void generateRandomTrackPlay() throws IOException {
    recordTrackPlay(getRandomUser(), getRandomSong());
  }

  /**
   * Create and record a track play of a random song for the specified user.
   *
   * @param user the user for which to generate a random track play.
   * @throws IOException in case of an error writing to the table.
   */
  public void generateRandomTrackPlayForUser(String user) throws IOException {
    recordTrackPlay(user, getRandomSong());
  }

  /**
   * Generate a new random track play for each user.
   *
   * @throws IOException in case of an error writing to the table.
   */
  public void generateRandomTrackPlayForAll() throws IOException {
    for (int x = 0; x < 50; x++) {
      recordTrackPlay(getUserForId(x), getRandomSong());
    }
  }

  /**
   * Create a record a track play of this specified song for a random user.
   * @param song
   * @throws IOException
   */
  public void generateRandomTrackPlayOfSong(String song) throws IOException {
    recordTrackPlay(getRandomUser(), song);
  }

  /**
   * While the thread has not been stopped, generate a new random track play after the specified
   * delay.
   */
  @Override
  public void run() {
    while (!mShouldStop) {
      try {
        generateRandomTrackPlay();
      } catch (IOException ioe) {
        // LOG IOExceptions and proceed.
        LOG.error(ioe.getMessage());
      }
      try {
        Thread.sleep(mDelay);
      } catch (InterruptedException ie) {
        // InterruptedExceptions should blow up the thread.
        throw new RuntimeInterruptedException(ie);
      }
    }
    try {
      close();
    } catch (IOException ioe) {
      // IOException finalizing resources should blow up the thread.
      throw new RuntimeIOException(ioe);
    }
  }

  /**
   * Get a map of the most recent track plays by user.  This is used by tests to confirm that data
   * retrieved from the table matches the data written.
   *
   * @return a map of the most recent track plays by user.
   */
  Map<String, String> getGeneratedPlays() {
    return mGenerated;
  }

  /** Tell the thread to stop generating new track plays. */
  public void stopGenerating() {
    mShouldStop = true;
  }

  /**
   * Close resources safely.
   *
   * @throws IOException in case of an error closing the KijiTableWriter or KijiTable.
   */
  private void close() throws IOException {
    mWriter.close();
    mTable.release();
  }
}
