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

package org.opensearch.client.indices;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;

/**
 * A request to create an index template.
 */
public class PutIndexTemplateRequest extends ClusterManagerNodeRequest<PutIndexTemplateRequest>
    implements
        IndicesRequest,
        ToXContentFragment {

    private String name;

    private String cause = "";

    private List<String> indexPatterns;

    /**
     * This field corresponds to the deprecated 'template' parameter, which was replaced by
     * 'index_patterns' in 6.0. It is stored and rendered to xContent separately from
     * 'index_patterns' to ensure that the server emits a deprecation warning when it's been set.
     */
    @Deprecated
    @Nullable
    private String template;

    private int order;

    private boolean create;

    private Settings settings = EMPTY_SETTINGS;

    private BytesReference mappings = null;

    private final Set<Alias> aliases = new HashSet<>();

    private Integer version;

    /**
     * Constructs a new put index template request with the provided name.
     */
    public PutIndexTemplateRequest(String name) {
        this.name(name);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (template == null && (indexPatterns == null || indexPatterns.size() == 0)) {
            validationException = addValidationError("index patterns are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the index template.
     */
    public PutIndexTemplateRequest name(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Name cannot be null");
        }
        this.name = name;
        return this;
    }

    /**
     * The name of the index template.
     */
    public String name() {
        return this.name;
    }

    public PutIndexTemplateRequest patterns(List<String> indexPatterns) {
        this.indexPatterns = indexPatterns;
        return this;
    }

    public List<String> patterns() {
        return this.indexPatterns;
    }

    public PutIndexTemplateRequest order(int order) {
        this.order = order;
        return this;
    }

    public int order() {
        return this.order;
    }

    public PutIndexTemplateRequest version(Integer version) {
        this.version = version;
        return this;
    }

    public Integer version() {
        return this.version;
    }

    /**
     * Set to {@code true} to force only creation, not an update of an index template. If it already
     * exists, it will fail with an {@link IllegalArgumentException}.
     */
    public PutIndexTemplateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public boolean create() {
        return create;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * The settings to create the index template with.
     */
    public PutIndexTemplateRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * The settings to create the index template with (either json/yaml format).
     */
    public PutIndexTemplateRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * The settings to create the index template with (either json or yaml format).
     */
    public PutIndexTemplateRequest settings(Map<String, Object> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    public Settings settings() {
        return this.settings;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param mediaType The type of content contained within the source
     */
    public PutIndexTemplateRequest mapping(String source, MediaType mediaType) {
        internalMapping(XContentHelper.convertToMap(new BytesArray(source), true, mediaType).v2());
        return this;
    }

    /**
     * The cause for this index template creation.
     */
    public PutIndexTemplateRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    public String cause() {
        return this.cause;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(XContentBuilder source) {
        internalMapping(XContentHelper.convertToMap(BytesReference.bytes(source), true, source.contentType()).v2());
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     * @param xContentType the source content type
     */
    public PutIndexTemplateRequest mapping(BytesReference source, XContentType xContentType) {
        internalMapping(XContentHelper.convertToMap(source, true, xContentType).v2());
        return this;
    }

    /**
     * Adds mapping that will be added when the index gets created.
     *
     * @param source The mapping source
     */
    public PutIndexTemplateRequest mapping(Map<String, Object> source) {
        return internalMapping(source);
    }

    private PutIndexTemplateRequest internalMapping(Map<String, Object> source) {
        try {
            XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder();
            builder.map(source);
            MediaType mediaType = builder.contentType();
            Objects.requireNonNull(mediaType);
            try {
                mappings = new BytesArray(XContentHelper.convertToJson(BytesReference.bytes(builder), false, false, mediaType));
                return this;
            } catch (IOException e) {
                throw new UncheckedIOException("failed to convert source to json", e);
            }
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public BytesReference mappings() {
        return this.mappings;
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(XContentBuilder templateBuilder) {
        try {
            return source(BytesReference.bytes(templateBuilder), templateBuilder.contentType());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to build json for template request", e);
        }
    }

    /**
     * The template source definition.
     */
    @SuppressWarnings("unchecked")
    public PutIndexTemplateRequest source(Map<String, Object> templateSource) {
        Map<String, Object> source = templateSource;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("template")) {
                if (entry.getValue() instanceof String) {
                    this.template = (String) entry.getValue();
                }
            } else if (name.equals("index_patterns")) {
                if (entry.getValue() instanceof String) {
                    patterns(Collections.singletonList((String) entry.getValue()));
                } else if (entry.getValue() instanceof List) {
                    List<String> elements = ((List<?>) entry.getValue()).stream().map(Object::toString).collect(Collectors.toList());
                    patterns(elements);
                } else {
                    throw new IllegalArgumentException("Malformed [index_patterns] value, should be a string or a list of strings");
                }
            } else if (name.equals("order")) {
                order(XContentMapValues.nodeIntegerValue(entry.getValue(), order()));
            } else if ("version".equals(name)) {
                if ((entry.getValue() instanceof Integer) == false) {
                    throw new IllegalArgumentException("Malformed [version] value, should be an integer");
                }
                version((Integer) entry.getValue());
            } else if (name.equals("settings")) {
                if ((entry.getValue() instanceof Map) == false) {
                    throw new IllegalArgumentException("Malformed [settings] section, should include an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("mappings")) {
                Map<String, Object> mappings = (Map<String, Object>) entry.getValue();
                mapping(mappings);
            } else if (name.equals("aliases")) {
                aliases((Map<String, Object>) entry.getValue());
            } else {
                throw new OpenSearchParseException("unknown key [{}] in the template ", name);
            }
        }
        return this;
    }

    /**
     * The template source definition.
     *
     * @deprecated use {@link #source(String, MediaType)} instead
     */
    @Deprecated
    public PutIndexTemplateRequest source(String templateSource, XContentType xContentType) {
        return source(XContentHelper.convertToMap(xContentType.xContent(), templateSource, true));
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(String templateSource, MediaType mediaType) {
        return source(XContentHelper.convertToMap(mediaType.xContent(), templateSource, true));
    }

    /**
     * The template source definition.
     *
     * @deprecated use {@link #source(byte[], MediaType)} instead
     */
    @Deprecated
    public PutIndexTemplateRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source, MediaType mediaType) {
        return source(source, 0, source.length, mediaType);
    }

    /**
     * The template source definition.
     *
     * @deprecated use {@link #source(byte[], int, int, MediaType)} instead
     */
    @Deprecated
    public PutIndexTemplateRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(byte[] source, int offset, int length, MediaType mediaType) {
        return source(new BytesArray(source, offset, length), mediaType);
    }

    /**
     * The template source definition.
     *
     * @deprecated use {@link #source(BytesReference, MediaType)} instead
     */
    @Deprecated
    public PutIndexTemplateRequest source(BytesReference source, XContentType xContentType) {
        return source(XContentHelper.convertToMap(source, true, xContentType).v2());
    }

    /**
     * The template source definition.
     */
    public PutIndexTemplateRequest source(BytesReference source, MediaType mediaType) {
        return source(XContentHelper.convertToMap(source, true, mediaType).v2());
    }

    public Set<Alias> aliases() {
        return this.aliases;
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(Map<String, ?> source) {
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
    public PutIndexTemplateRequest aliases(XContentBuilder source) {
        return aliases(BytesReference.bytes(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(String source) {
        return aliases(new BytesArray(source));
    }

    /**
     * Sets the aliases that will be associated with the index when it gets created
     */
    public PutIndexTemplateRequest aliases(BytesReference source) {
        // EMPTY is safe here because we never call namedObject
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source
            )
        ) {
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
     * Adds an alias that will be added when the index gets created.
     *
     * @param alias   The metadata for the new alias
     * @return  the index template creation request
     */
    public PutIndexTemplateRequest alias(Alias alias) {
        aliases.add(alias);
        return this;
    }

    @Override
    public String[] indices() {
        return indexPatterns.toArray(new String[0]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictExpand();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (template != null) {
            builder.field("template", template);
        } else {
            builder.field("index_patterns", indexPatterns);
        }

        builder.field("order", order);
        if (version != null) {
            builder.field("version", version);
        }

        builder.startObject("settings");
        settings.toXContent(builder, params);
        builder.endObject();

        if (mappings != null) {
            builder.field("mappings");
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    mappings.utf8ToString()
                )
            ) {
                builder.copyCurrentStructure(parser);
            }
        }

        builder.startObject("aliases");
        for (Alias alias : aliases) {
            alias.toXContent(builder, params);
        }
        builder.endObject();

        return builder;
    }
}
