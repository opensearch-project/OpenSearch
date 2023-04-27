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

package org.opensearch.action.admin.indices.get;

import org.opensearch.Version;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A response for a get index action.
 *
 * @opensearch.internal
 */
public class GetIndexResponse extends ActionResponse implements ToXContentObject {

    private Map<String, MappingMetadata> mappings = Map.of();
    private Map<String, List<AliasMetadata>> aliases = Map.of();
    private Map<String, Settings> settings = Map.of();
    private Map<String, Settings> defaultSettings = Map.of();
    private Map<String, String> dataStreams = Map.of();
    private final String[] indices;

    public GetIndexResponse(
        String[] indices,
        Map<String, MappingMetadata> mappings,
        final Map<String, List<AliasMetadata>> aliases,
        final Map<String, Settings> settings,
        final Map<String, Settings> defaultSettings,
        final Map<String, String> dataStreams
    ) {
        this.indices = indices;
        // to have deterministic order
        Arrays.sort(indices);
        if (mappings != null) {
            this.mappings = mappings;
        }
        if (aliases != null) {
            this.aliases = Collections.unmodifiableMap(aliases);
        }
        if (settings != null) {
            this.settings = Collections.unmodifiableMap(settings);
        }
        if (defaultSettings != null) {
            this.defaultSettings = Collections.unmodifiableMap(defaultSettings);
        }
        if (dataStreams != null) {
            this.dataStreams = Collections.unmodifiableMap(dataStreams);
        }
    }

    GetIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();

        int mappingsSize = in.readVInt();
        Map<String, MappingMetadata> mappingsMapBuilder = new HashMap<>();
        for (int i = 0; i < mappingsSize; i++) {
            String index = in.readString();
            if (in.getVersion().before(Version.V_2_0_0)) {
                int numMappings = in.readVInt();
                if (numMappings == 0) {
                    mappingsMapBuilder.put(index, MappingMetadata.EMPTY_MAPPINGS);
                } else if (numMappings == 1) {
                    String type = in.readString();
                    if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
                        throw new IllegalStateException("Expected " + MapperService.SINGLE_MAPPING_NAME + " but got [" + type + "]");
                    }
                    mappingsMapBuilder.put(index, new MappingMetadata(in));
                } else {
                    throw new IllegalStateException("Expected 0 or 1 mappings but got: " + numMappings);
                }
            } else {
                final MappingMetadata metadata = in.readOptionalWriteable(MappingMetadata::new);
                mappingsMapBuilder.put(index, metadata != null ? metadata : MappingMetadata.EMPTY_MAPPINGS);
            }
        }
        mappings = Collections.unmodifiableMap(mappingsMapBuilder);

        int aliasesSize = in.readVInt();
        final Map<String, List<AliasMetadata>> aliasesMapBuilder = new HashMap<>();
        for (int i = 0; i < aliasesSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            List<AliasMetadata> aliasEntryBuilder = new ArrayList<>(valueSize);
            for (int j = 0; j < valueSize; j++) {
                aliasEntryBuilder.add(new AliasMetadata(in));
            }
            aliasesMapBuilder.put(key, Collections.unmodifiableList(aliasEntryBuilder));
        }
        aliases = Collections.unmodifiableMap(aliasesMapBuilder);

        int settingsSize = in.readVInt();
        final Map<String, Settings> settingsMapBuilder = new HashMap<>();
        for (int i = 0; i < settingsSize; i++) {
            String key = in.readString();
            settingsMapBuilder.put(key, Settings.readSettingsFromStream(in));
        }
        settings = Collections.unmodifiableMap(settingsMapBuilder);

        final Map<String, Settings> defaultSettingsMapBuilder = new HashMap<>();
        int defaultSettingsSize = in.readVInt();
        for (int i = 0; i < defaultSettingsSize; i++) {
            defaultSettingsMapBuilder.put(in.readString(), Settings.readSettingsFromStream(in));
        }
        defaultSettings = Collections.unmodifiableMap(defaultSettingsMapBuilder);

        final Map<String, String> dataStreamsMapBuilder = new HashMap<>();
        int dataStreamsSize = in.readVInt();
        for (int i = 0; i < dataStreamsSize; i++) {
            dataStreamsMapBuilder.put(in.readString(), in.readOptionalString());
        }
        dataStreams = Collections.unmodifiableMap(dataStreamsMapBuilder);
    }

    public String[] indices() {
        return indices;
    }

    public String[] getIndices() {
        return indices();
    }

    public Map<String, MappingMetadata> mappings() {
        return mappings;
    }

    public Map<String, MappingMetadata> getMappings() {
        return mappings();
    }

    public Map<String, List<AliasMetadata>> aliases() {
        return aliases;
    }

    public Map<String, List<AliasMetadata>> getAliases() {
        return aliases();
    }

    public Map<String, Settings> settings() {
        return settings;
    }

    public Map<String, String> dataStreams() {
        return dataStreams;
    }

    public Map<String, String> getDataStreams() {
        return dataStreams();
    }

    /**
     * If the originating {@link GetIndexRequest} object was configured to include
     * defaults, this will contain a mapping of index name to {@link Settings} objects.
     * The returned {@link Settings} objects will contain only those settings taking
     * effect as defaults.  Any settings explicitly set on the index will be available
     * via {@link #settings()}.
     * See also {@link GetIndexRequest#includeDefaults(boolean)}
     */
    public Map<String, Settings> defaultSettings() {
        return defaultSettings;
    }

    public Map<String, Settings> getSettings() {
        return settings();
    }

    /**
     * Returns the string value for the specified index and setting.  If the includeDefaults flag was not set or set to
     * false on the {@link GetIndexRequest}, this method will only return a value where the setting was explicitly set
     * on the index.  If the includeDefaults flag was set to true on the {@link GetIndexRequest}, this method will fall
     * back to return the default value if the setting was not explicitly set.
     */
    public String getSetting(String index, String setting) {
        Settings indexSettings = settings.get(index);
        if (setting != null) {
            if (indexSettings != null && indexSettings.hasValue(setting)) {
                return indexSettings.get(setting);
            } else {
                Settings defaultIndexSettings = defaultSettings.get(index);
                if (defaultIndexSettings != null) {
                    return defaultIndexSettings.get(setting);
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        out.writeVInt(mappings.size());
        for (final Map.Entry<String, MappingMetadata> indexEntry : mappings.entrySet()) {
            out.writeString(indexEntry.getKey());
            if (out.getVersion().before(Version.V_2_0_0)) {
                out.writeVInt(indexEntry.getValue() == MappingMetadata.EMPTY_MAPPINGS ? 0 : 1);
                if (indexEntry.getValue() != MappingMetadata.EMPTY_MAPPINGS) {
                    out.writeString(MapperService.SINGLE_MAPPING_NAME);
                    indexEntry.getValue().writeTo(out);
                }
            } else {
                out.writeOptionalWriteable(indexEntry.getValue());
            }
        }
        out.writeVInt(aliases.size());
        for (final Map.Entry<String, List<AliasMetadata>> indexEntry : aliases.entrySet()) {
            out.writeString(indexEntry.getKey());
            out.writeVInt(indexEntry.getValue().size());
            for (AliasMetadata aliasEntry : indexEntry.getValue()) {
                aliasEntry.writeTo(out);
            }
        }
        out.writeVInt(settings.size());
        for (final Map.Entry<String, Settings> indexEntry : settings.entrySet()) {
            out.writeString(indexEntry.getKey());
            Settings.writeSettingsToStream(indexEntry.getValue(), out);
        }
        out.writeVInt(defaultSettings.size());
        for (final Map.Entry<String, Settings> indexEntry : defaultSettings.entrySet()) {
            out.writeString(indexEntry.getKey());
            Settings.writeSettingsToStream(indexEntry.getValue(), out);
        }
        out.writeVInt(dataStreams.size());
        for (final Map.Entry<String, String> indexEntry : dataStreams.entrySet()) {
            out.writeString(indexEntry.getKey());
            out.writeOptionalString(indexEntry.getValue());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            for (final String index : indices) {
                builder.startObject(index);
                {
                    builder.startObject("aliases");
                    List<AliasMetadata> indexAliases = aliases.get(index);
                    if (indexAliases != null) {
                        for (final AliasMetadata alias : indexAliases) {
                            AliasMetadata.Builder.toXContent(alias, builder, params);
                        }
                    }
                    builder.endObject();

                    MappingMetadata indexMappings = mappings.get(index);
                    if (indexMappings == null) {
                        builder.startObject("mappings").endObject();
                    } else {
                        builder.field("mappings", indexMappings.sourceAsMap());
                    }

                    builder.startObject("settings");
                    Settings indexSettings = settings.get(index);
                    if (indexSettings != null) {
                        indexSettings.toXContent(builder, params);
                    }
                    builder.endObject();

                    Settings defaultIndexSettings = defaultSettings.get(index);
                    if (defaultIndexSettings != null && defaultIndexSettings.isEmpty() == false) {
                        builder.startObject("defaults");
                        defaultIndexSettings.toXContent(builder, params);
                        builder.endObject();
                    }

                    String dataStream = dataStreams.get(index);
                    if (dataStream != null) {
                        builder.field("data_stream", dataStream);
                    }
                }
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetIndexResponse that = (GetIndexResponse) o;
        return Arrays.equals(indices, that.indices)
            && Objects.equals(aliases, that.aliases)
            && Objects.equals(mappings, that.mappings)
            && Objects.equals(settings, that.settings)
            && Objects.equals(defaultSettings, that.defaultSettings)
            && Objects.equals(dataStreams, that.dataStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), aliases, mappings, settings, defaultSettings, dataStreams);
    }
}
