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

package org.opensearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ThreadLeakFilters(filters = HdfsClientThreadLeakFilter.class)
public class HdfsRepositoryTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(HdfsPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        return new MockSecureSettings();
    }

    @Override
    protected void createRepository(String repoName) {
        Settings.Builder settings = Settings.builder()
            .put("uri", "hdfs:///")
            .put("conf.fs.AbstractFileSystem.hdfs.impl", TestingFs.class.getName())
            .put("path", "foo")
            .put("chunk_size", randomIntBetween(100, 1000) + "k")
            .put("compress", randomBoolean());
        OpenSearchIntegTestCase.putRepository(client().admin().cluster(), repoName, "hdfs", settings);
    }

    // HDFS repository doesn't have precise cleanup stats so we only check whether or not any blobs were removed
    @Override
    protected void assertCleanupResponse(CleanupRepositoryResponse response, long bytes, long blobs) {
        if (blobs > 0) {
            assertThat(response.result().blobs(), greaterThan(0L));
        } else {
            assertThat(response.result().blobs(), equalTo(0L));
        }
    }
}
