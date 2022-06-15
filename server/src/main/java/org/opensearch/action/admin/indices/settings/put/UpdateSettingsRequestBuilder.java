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

package org.opensearch.action.admin.indices.settings.put;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequestBuilder;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;

import java.util.Map;

/**
 * Builder for an update index settings request
 *
 * @opensearch.internal
 */
public class UpdateSettingsRequestBuilder extends AcknowledgedRequestBuilder<
    UpdateSettingsRequest,
    AcknowledgedResponse,
    UpdateSettingsRequestBuilder> {

    public UpdateSettingsRequestBuilder(OpenSearchClient client, UpdateSettingsAction action, String... indices) {
        super(client, action, new UpdateSettingsRequest(indices));
    }

    /**
     * Sets the indices the update settings will execute on
     */
    public UpdateSettingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public UpdateSettingsRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    public UpdateSettingsRequestBuilder setPreserveExisting(boolean preserveExisting) {
        request.setPreserveExisting(preserveExisting);
        return this;
    }
}
