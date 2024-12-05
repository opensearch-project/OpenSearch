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

package org.opensearch.repositories.gcs;

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Base64;
import java.util.Collection;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class GoogleCloudStorageThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());

        if (Strings.isNullOrEmpty(System.getProperty("test.google.endpoint")) == false) {
            builder.put("gcs.client.default.endpoint", System.getProperty("test.google.endpoint"));
        }

        if (Strings.isNullOrEmpty(System.getProperty("test.google.tokenURI")) == false) {
            builder.put("gcs.client.default.token_uri", System.getProperty("test.google.tokenURI"));
        }

        return builder.build();
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.google.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.google.bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(
            "gcs.client.default.credentials_file",
            Base64.getDecoder().decode(System.getProperty("test.google.account"))
        );
        return secureSettings;
    }

    @Override
    protected void createRepository(final String repoName) {
        Settings.Builder settings = Settings.builder()
            .put("bucket", System.getProperty("test.google.bucket"))
            .put("base_path", System.getProperty("test.google.base", "/"));
        OpenSearchIntegTestCase.putRepository(client().admin().cluster(), "test-repo", "gcs", settings);
    }
}
