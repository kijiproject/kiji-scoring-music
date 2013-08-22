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

/**
 * This is the top level package for the KijiScoring Music tutorial.
 * Classes:
 *   KVStoreMaskingAlwaysFreshen - An AlwaysFreshen KijiFreshnessPolicy which can be configured with
 *       a KijiTable KVStore which masks an unconfigured KVStore for a KijiProducer.
 *   TrackPlayGenerator - A Thread which generates new track play entries in the users table to
 *       demonstrate how KijiScoring responds to data changing in real time.
 */
package org.kiji.scoring.music;
