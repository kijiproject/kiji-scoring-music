package org.kiji.scoring.music;

import java.util.List;

import com.google.common.base.Preconditions;

import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshKijiTableReaderBuilder;

public class ScoringMusicTool extends BaseTool {

  @Flag(name="kiji", usage="the KijiURI of the kiji music tutorial instance.")
  private String mURIFlag = "";

  @Flag(name="write-user", usage="specify the user for which to generate a new random track play. "
      + "user names are of the format 'user-#' where # is a value between 0 and 49 inclusive with "
      + "no leading zero.")
  private String mWriteUserFlag = "";

  @Flag(name="write-all", usage="Generate a new random track play for all users.")
  private boolean mWriteAllFlag;

  @Flag(name="freshen-user", usage="specify the user for which to perform a freshened read request."
      + " User names are of the format 'user-#' where # is a value between 0 and 49 inclusive with "
      + "no leading zero. If specificed with a writing flag, this command will be executed last.")
  private String mFreshenUserFlag = "";

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
        if (userId >= 0 && userId < 50) {
          // Number is in legal range and the flag starts with "user-" so it is a legal username.
          return true;
        } else {
          // Number is outside of legal range.
          return false;
        }
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
    Preconditions.checkArgument(!mURIFlag.isEmpty(),
        "Please specify the KijiURI of the music tutorial instance. If running the tutorial in the "
        + "BentoBox, this URI should be in the environment variable $KIJI");
    mURI = KijiURI.newBuilder(mURIFlag).build();
    Preconditions.checkArgument(
        mWriteAllFlag || ! mWriteUserFlag.isEmpty() || !mFreshenUserFlag.isEmpty(),
        "Please specify at least one of --write-all, --write-user, or --freshen-user.");
    if (!mWriteUserFlag.isEmpty()) {
      Preconditions.checkArgument(isValidUser(mWriteUserFlag), String.format(
          "Specified user: '%s' is invalid. A user name must follow the format 'user-#' where # is "
          + "a value between 0 and 49 inclusive with no leading zero.", mWriteUserFlag));
    }
    if (!mFreshenUserFlag.isEmpty()) {
      Preconditions.checkArgument(isValidUser(mFreshenUserFlag), String.format(
          "Specified user: '%s' is invalid. A user name must follow the format 'user-#' where # is "
              + "a value between 0 and 49 inclusive with no leading zero.", mFreshenUserFlag));
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(final List<String> nonFlagArgs) throws Exception {
    final TrackPlayGenerator generator = new TrackPlayGenerator(mURI, 10);
    if (mWriteAllFlag) {
      generator.generateRandomTrackPlayForAll();
    }
    if (!mWriteUserFlag.isEmpty()) {
      generator.generateRandomTrackPlayForUser(mWriteUserFlag);
    }
    if (!mFreshenUserFlag.isEmpty()) {
      final Kiji kiji = Kiji.Factory.open(mURI);
      try {
        final KijiTable table = kiji.openTable("user");
        try {
          final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
              .withTable(table)
              .withTimeout(500)
              .build();
          try {
            final EntityId eid = table.getEntityId(mFreshenUserFlag);
            final KijiDataRequest request = KijiDataRequest.create("info", "next_song_rec");
            final KijiRowData freshRecommendation =
                freshReader.get(eid, request).getMostRecentValue("info", "next_song_rec");
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
