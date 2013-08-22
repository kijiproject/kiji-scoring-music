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

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.PolicyContext;

/**
 * A KijiFreshnessPolicy which always returns stale and which can be configured to use a KijiTable
 * KVStore.  This KVStore will also be available to the KijiProducer with which this policy is
 * attached.
 */
public final class KVStoreMaskingAlwaysFreshen implements KijiFreshnessPolicy {

  /**
   * Tuple object for storing configuration necessary to instantiate a
   * {@link KijiTableKeyValueStore}.
   */
  static final class KijiTableKVStoreConfig {
    /** The name by which the KVStore will be retrieved. */
    private final String mKVStoreName;
    /** The KijiURI of the table from which the KVStore will read values. */
    private final String mKVStoreTableURI;
    /** The column within the KijiTable from which the KVStore will read values. */
    private final String mKVStoreColumnName;

    /**
     * Default constructor.
     *
     * @param kvStoreName the name of the KVStore represented by this configuration.
     * @param kvStoreTableURI the TableURI of the table which backs the KVStore represented by this
     *     configuration.
     * @param kvStoreColumnName the column within the table which backs the KVStore represented by
     *     this configuration.
     */
    public KijiTableKVStoreConfig(
        final String kvStoreName,
        final String kvStoreTableURI,
        final String kvStoreColumnName) {
      mKVStoreName = kvStoreName;
      mKVStoreTableURI = kvStoreTableURI;
      mKVStoreColumnName = kvStoreColumnName;
    }

    /**
     * Get the name of the KVStore configured by this object.
     *
     * @return the name of the KVStore configured by this object.
     */
    public String getKVStoreName() {
      return mKVStoreName;
    }

    /**
     * Get the KijiURI of the table from which the KVStore configured by this object will read.
     *
     * @return the KijiURI of the table from which the KVStore configured by this object will read.
     */
    public String getKVStoreTableURI() {
      return mKVStoreTableURI;
    }

    /**
     * Get the name of the column from which the KVStore configured by this object will read.
     *
     * @return the name of the column from which the KVStore configured by this object will read.
     */
    public String getKVStoreColumnName() {
      return mKVStoreColumnName;
    }
  }

  /** All configuration information necessary to instantiate a KijiTableKVStore. */
  private KijiTableKVStoreConfig mKVStoreConfig = null;

  /** {@inheritDoc} */
  @Override
  public boolean isFresh(final KijiRowData rowData, final PolicyContext policyContext) {
    // This is an "AlwaysFreshen" freshness policy, so data is never fresh.
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean shouldUseClientDataRequest() {
    // This freshness policy does not need any special data for its isFresh() method so it uses the
    // client data request.
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // Because shouldUseClientDataRequest() returns true, this method will never be called and can
    // be left null.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    // Build a new KijiTable KVStore which will mask NextSongRecommender's unconfigured KVStore.
    final KeyValueStore<?, ?> kvstore = KijiTableKeyValueStore.builder()
        .withTable(KijiURI.newBuilder(mKVStoreConfig.getKVStoreTableURI()).build())
        .withColumn(new KijiColumnName(mKVStoreConfig.getKVStoreColumnName()))
        .build();

    final Map<String, KeyValueStore<?, ?>> stores = Maps.newHashMap();
    stores.put(mKVStoreConfig.getKVStoreName(), kvstore);
    return stores;
  }

  /** {@inheritDoc} */
  @Override
  public String serialize() {
    // Store the KVStore configuration information as JSON.  This JSON will be saved to the meta
    // table servicing your Kiji instance.
    final Gson gson = new Gson();
    final String json = gson.toJson(mKVStoreConfig);
    return json;
  }

  /** {@inheritDoc} */
  @Override
  public void deserialize(final String policyState) {
    // Restore the saved state.  This string will be retrieved from the meta table servicing your
    // Kiji instance.
    final Gson gson = new Gson();
    mKVStoreConfig = gson.fromJson(policyState, KijiTableKVStoreConfig.class);
  }
}
