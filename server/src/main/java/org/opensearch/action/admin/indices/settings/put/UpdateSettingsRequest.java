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

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * Request for an update index settings action
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class UpdateSettingsRequest extends AcknowledgedRequest<UpdateSettingsRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);
    private Settings settings = EMPTY_SETTINGS;
    private boolean preserveExisting = false;

    public UpdateSettingsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        settings = readSettingsFromStream(in);
        preserveExisting = in.readBoolean();
    }

    public UpdateSettingsRequest() {}

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(Settings settings, String... indices) {
        this.indices = indices;
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (settings.isEmpty()) {
            validationException = addValidationError("no settings to update", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    public Settings settings() {
        return settings;
    }

    /**
     * Sets the indices to apply to settings update to
     */
    @Override
    public UpdateSettingsRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public UpdateSettingsRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequest settings(String source, MediaType mediaType) {
        this.settings = Settings.builder().loadFromSource(source, mediaType).build();
        return this;
    }

    /**
     * Returns <code>true</code> iff the settings update should only add but not update settings. If the setting already exists
     * it should not be overwritten by this update. The default is <code>false</code>
     */
    public boolean isPreserveExisting() {
        return preserveExisting;
    }

    /**
     * Iff set to <code>true</code> this settings update will only add settings not already set on an index. Existing settings remain
     * unchanged.
     */
    public UpdateSettingsRequest setPreserveExisting(boolean preserveExisting) {
        this.preserveExisting = preserveExisting;
        return this;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequest settings(Map<String, ?> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        writeSettingsToStream(settings, out);
        out.writeBoolean(preserveExisting);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        settings.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public UpdateSettingsRequest fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> settings = new HashMap<>();
        Map<String, Object> bodySettings = parser.map();
        Object innerBodySettings = bodySettings.get("settings");
        // clean up in case the body is wrapped with "settings" : { ... }
        if (innerBodySettings instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> innerBodySettingsMap = (Map<String, Object>) innerBodySettings;
            settings.putAll(innerBodySettingsMap);
        } else {
            settings.putAll(bodySettings);
        }
        return this.settings(settings);
    }

    @Override
    public String toString() {
        return "indices : " + Arrays.toString(indices) + "," + Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdateSettingsRequest that = (UpdateSettingsRequest) o;
        return clusterManagerNodeTimeout.equals(that.clusterManagerNodeTimeout)
            && timeout.equals(that.timeout)
            && Objects.equals(settings, that.settings)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(preserveExisting, that.preserveExisting)
            && Arrays.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterManagerNodeTimeout, timeout, settings, indicesOptions, preserveExisting, Arrays.hashCode(indices));
    }

}
