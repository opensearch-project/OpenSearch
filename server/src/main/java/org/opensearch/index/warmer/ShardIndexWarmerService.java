/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.warmer;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.AbstractIndexShardComponent;

import java.util.concurrent.TimeUnit;

/**
 * Warms the index into the cache
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardIndexWarmerService extends AbstractIndexShardComponent {

    private final CounterMetric current = new CounterMetric();
    private final MeanMetric warmerMetric = new MeanMetric();

    public ShardIndexWarmerService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
    }

    public Logger logger() {
        return this.logger;
    }

    public void onPreWarm() {
        current.inc();
    }

    public void onPostWarm(long tookInNanos) {
        current.dec();
        warmerMetric.inc(tookInNanos);
    }

    public WarmerStats stats() {
        return new WarmerStats(current.count(), warmerMetric.count(), TimeUnit.NANOSECONDS.toMillis(warmerMetric.sum()));
    }
}
