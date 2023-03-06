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

package org.opensearch.action.admin.indices.create;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * A request to create an index. Best created with {@link org.opensearch.client.Requests#createIndexRequest(String)}.
 * <p>
 * The index created can optionally be created with {@link #settings(org.opensearch.common.settings.Settings)}.
 *
 * @see org.opensearch.client.IndicesAdminClient#create(CreateIndexRequest)
 * @see org.opensearch.client.Requests#createIndexRequest(String)
 * @see CreateIndexResponse
 *
 * @opensearch.internal
 */
public class CreateIndexRequest extends AcknowledgedRequest<CreateIndexRequest> implements IndicesRequest {

    public static final ParseField MAPPINGS = new ParseField("mappings");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField ALIASES = new ParseField("aliases");

    private String cause = "";

    private String index;

    private Settings settings = EMPTY_SETTINGS;

    private String mappings = "{}";

    private final Set<Alias> aliases = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    public CreateIndexRequest(StreamInput in) throws IOException {
        super(in);
        cause = in.readString();
        index = in.readString();
        settings = readSettingsFromStream(in);
        if (in.getVersion().before(Version.V_2_0_0)) {
            int size = in.readVInt();
            if (size == 1) {
                String type = in.readString();
                if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
                    throw new IllegalArgumentException(
                        "Expected to receive mapping type of [" + MapperService.SINGLE_MAPPING_NAME + "] but got [" + type + "]"
                    );
                }
                mappings = in.readString();
            } else if (size != 0) {
                throw new IllegalStateException("Expected to read 0 or 1 mappings, but received " + size);
            }
        } else {
            mappings = in.readString();
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            aliases.add(new Alias(in));
        }
        waitForActiveShards = ActiveShardCount.readFrom(in);
    }

    public CreateIndexRequest() {}

    /**
     * Constructs a new request to create an index with the specified name.
     */
    public CreateIndexRequest(String index) {
        this(index, EMPTY_SETTINGS);
    }

    /**
     * Constructs a new request to create an index with the specified name and settings.
     */
    public CreateIndexRequest(String index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    /**
     * The index name to create.
     */
    public String index() {
        return index;
    }

    public CreateIndexRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public Settings settings() {
        return settings;
    }

    /**
     * The cause for this index creation.
     */
    public String cause() {
        return cause;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index with.
     */
    public CreateIndexRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index with (either json or yaml format)
     *
     * @deprecated use {@link #settings(String source, MediaType mediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * The settings to create the index with (using a generic MediaType)
     */
    private CreateIndexRequest settings(String source, MediaType mediaType) {
        this.settings = Settings.builder().loadFromSource(source, mediaType).build();
        return this;
    }

    /**
     * Allows to set the settings using a json builder.
     */
    public CreateIndexRequest settings(XContentBuilder builder) {
        settings(Strings.toString(builder), builder.contentType());
        return this;
    }

    /**
     * The settings to create the index with (either json/yaml/properties format)
     */
    public CreateIndexRequest settings(Map<String, ?> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Set the mapping for this index
     *
     * The mapping should be in the form of a JSON string, with an outer _doc key
     * <pre>
     *     .mapping("{\"_doc\":{\"properties\": ... }}")
     * </pre>
     */
    public CreateIndexRequest mapping(String mapping) {
        this.mappings = mapping;
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param xContentType The content type of the source
     *
     * @deprecated use {@link #mapping(String source, MediaType mediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest mapping(String source, XContentType xContentType) {
        return mapping(new BytesArray(source), xContentType);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     * @param mediaType The media type of the source
     */
    public CreateIndexRequest mapping(String source, MediaType mediaType) {
        return mapping(new BytesArray(source), mediaType);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param xContentType the content type of the mapping source
     *
     * @deprecated use {@link #mapping(BytesReference source, MediaType mediaType)} instead
     */
    @Deprecated
    private CreateIndexRequest mapping(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        Map<String, Object> mappingAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
        return mapping(MapperService.SINGLE_MAPPING_NAME, mappingAsMap);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * Note that the definition should *not* be nested under a type name.
     *
     * @param source The mapping source
     * @param mediaType The media type of the source
     */
    public CreateIndexRequest mapping(BytesReference source, MediaType mediaType) {
        Objects.requireNonNull(mediaType);
        Map<String, Object> mappingAsMap = XContentHelper.convertToMap(source, false, mediaType).v2();
        return mapping(MapperService.SINGLE_MAPPING_NAME, mappingAsMap);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(XContentBuilder source) {
        return mapping(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Set the mapping for this index
     *
     * @param source The mapping source
     */
    public CreateIndexRequest mapping(Map<String, ?> source) {
        return mapping(MapperService.SINGLE_MAPPING_NAME, source);
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     * @deprecated types are being removed
     */
    @Deprecated
    private CreateIndexRequest mapping(String type, Map<String, ?> source) {
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, source);
        } else if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
            // if it has a different type name, then unwrap and rewrap with _doc
            source = Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, source.get(type));
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return mapping(Strings.toString(builder));
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public CreateIndexRequest simpleMapping(String... source) {
        mapping(PutMappingRequest.simpleMapping(source));
        return this;
    }

    /**
     * The cause for this index creation.
     */
    public CreateIndexRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(Map<String, ?> source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return aliases(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(XContentBuilder source) {
        return aliases(BytesReference.bytes(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public CreateIndexRequest aliases(BytesReference source) {
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, source)) {
            // move to the first alias
            parser.nextToken();
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                alias(Alias.fromXContent(parser));
            }
            return this;
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse aliases", e);
        }
    }

    /**
     * Adds an alias that will be associated with the index when it gets created
     */
    public CreateIndexRequest alias(Alias alias) {
        this.aliases.add(alias);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @deprecated use {@link #source(String, MediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(String source, XContentType xContentType) {
        return source(new BytesArray(source), xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * Note that the mapping definition should *not* be nested under a type name.
     */
    public CreateIndexRequest source(String source, MediaType mediaType) {
        return source(new BytesArray(source), mediaType);
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(XContentBuilder source) {
        return source(BytesReference.bytes(source), source.contentType());
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @deprecated use {@link #source(byte[], MediaType mediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * Note that the mapping definition should *not* be nested under a type name.
     */
    public CreateIndexRequest source(byte[] source, MediaType mediaType) {
        return source(source, 0, source.length, mediaType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @deprecated use {@link #source(byte[], int, int, MediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(byte[] source, int offset, int length, MediaType mediaType) {
        return source(new BytesArray(source, offset, length), mediaType);
    }

    /**
     * Sets the settings and mappings as a single source.
     *
     * @deprecated use {@link #source(BytesReference, MediaType)} instead
     */
    @Deprecated
    public CreateIndexRequest source(BytesReference source, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        source(XContentHelper.convertToMap(source, false, xContentType).v2(), LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    public CreateIndexRequest source(BytesReference source, MediaType mediaType) {
        Objects.requireNonNull(mediaType);
        source(XContentHelper.convertToMap(source, false, mediaType).v2(), LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    /**
     * Sets the settings and mappings as a single source.
     */
    @SuppressWarnings("unchecked")
    public CreateIndexRequest source(Map<String, ?> source, DeprecationHandler deprecationHandler) {
        for (Map.Entry<String, ?> entry : source.entrySet()) {
            String name = entry.getKey();
            if (SETTINGS.match(name, deprecationHandler)) {
                if (entry.getValue() instanceof Map == false) {
                    throw new OpenSearchParseException("key [settings] must be an object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (MAPPINGS.match(name, deprecationHandler)) {
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                for (Map.Entry<String, Object> entry1 : mappings.entrySet()) {
                    mapping(entry1.getKey(), (Map<String, Object>) entry1.getValue());
                }
            } else if (ALIASES.match(name, deprecationHandler)) {
                aliases((Map<String, Object>) entry.getValue());
            } else {
                throw new OpenSearchParseException("unknown key [{}] for create index", name);
            }
        }
        return this;
    }

    public String mappings() {
        return this.mappings;
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that should be active for index creation to return.
     * Defaults to {@link ActiveShardCount#DEFAULT}, which will wait for one shard copy
     * (the primary) to become active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active before returning.
     * Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link CreateIndexResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public CreateIndexRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public CreateIndexRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cause);
        out.writeString(index);
        writeSettingsToStream(settings, out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            if ("{}".equals(mappings)) {
                out.writeVInt(0);
            } else {
                out.writeVInt(1);
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
                out.writeString(mappings);
            }
        } else {
            out.writeString(mappings);
        }
        out.writeVInt(aliases.size());
        for (Alias alias : aliases) {
            alias.writeTo(out);
        }
        waitForActiveShards.writeTo(out);
    }
}
