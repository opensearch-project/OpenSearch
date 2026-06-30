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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.engine;

import org.apache.lucene.index.LiveIndexWriterConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.EngineAccess;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class InternalEngineSettingsTests extends OpenSearchSingleNodeTestCase {

    public void testSettingsUpdate() {
        final IndexService service = createIndex("foo");
        InternalEngine engine = ((InternalEngine) EngineAccess.engine(service.getShardOrNull(0)));
        assertThat(engine.getCurrentIndexWriterConfig().getUseCompoundFile(), is(true));
        final int iters = between(1, 20);
        for (int i = 0; i < iters; i++) {

            // Tricky: TimeValue.parseTimeValue casts this long to a double, which steals 11 of the 64 bits for exponent, so we can't use
            // the full long range here else the assert below fails:
            long gcDeletes = random().nextLong() & (Long.MAX_VALUE >> 11);

            Settings build = Settings.builder()
                .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), gcDeletes, TimeUnit.MILLISECONDS)
                .build();
            assertEquals(gcDeletes, build.getAsTime(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), null).millis());

            client().admin().indices().prepareUpdateSettings("foo").setSettings(build).get();
            LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();
            assertEquals(currentIndexWriterConfig.getUseCompoundFile(), true);

            assertEquals(engine.config().getIndexSettings().getGcDeletesInMillis(), gcDeletes);
            assertEquals(engine.getGcDeletesInMillis(), gcDeletes);

        }

        Settings settings = Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), 1000, TimeUnit.MILLISECONDS).build();
        client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 1000);
        assertTrue(engine.config().isEnableGcDeletes());

        settings = Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "0ms").build();

        client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 0);
        assertTrue(engine.config().isEnableGcDeletes());

        settings = Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), 1000, TimeUnit.MILLISECONDS).build();
        client().admin().indices().prepareUpdateSettings("foo").setSettings(settings).get();
        assertEquals(engine.getGcDeletesInMillis(), 1000);
        assertTrue(engine.config().isEnableGcDeletes());
    }
}
