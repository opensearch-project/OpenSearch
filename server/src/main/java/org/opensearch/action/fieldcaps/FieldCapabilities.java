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

package org.opensearch.action.fieldcaps;

import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Describes the capabilities of a field optionally merged across multiple indices.
 *
 * @opensearch.internal
 */
public class FieldCapabilities implements Writeable, ToXContentObject {

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField IS_ALIAS_FIELD = new ParseField("alias");
    private static final ParseField SEARCHABLE_FIELD = new ParseField("searchable");
    private static final ParseField AGGREGATABLE_FIELD = new ParseField("aggregatable");
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField ALIASES_FIELD = new ParseField("aliases");
    private static final ParseField NON_SEARCHABLE_INDICES_FIELD = new ParseField("non_searchable_indices");
    private static final ParseField NON_AGGREGATABLE_INDICES_FIELD = new ParseField("non_aggregatable_indices");
    private static final ParseField ALIAS_INDICES_FIELD = new ParseField("alias_indices");
    private static final ParseField META_FIELD = new ParseField("meta");

    private final String name;
    private final String type;
    private final boolean isSearchable;
    private final boolean isAlias;
    private final boolean isAggregatable;

    private final String[] indices;

    private final String[] aliases;
    private final String[] nonSearchableIndices;
    private final String[] aliasesIndices;
    private final String[] nonAggregatableIndices;

    private final Map<String, Set<String>> meta;

    /**
     * Constructor for a set of indices.
     *
     * @param name                   The name of the field
     * @param type                   The type associated with the field.
     * @param isAlias                Whether this field is an alias for another field.
     * @param isSearchable           Whether this field is indexed for search.
     * @param isAggregatable         Whether this field can be aggregated on.
     * @param indices                The list of indices where this field name is defined as {@code type},
     *                               or null if all indices have the same {@code type} for the field.
     * @param nonSearchableIndices   The list of indices where this field is not searchable,
     *                               or null if the field is searchable in all indices.
     * @param nonAggregatableIndices The list of indices where this field is not aggregatable,
     *                               or null if the field is aggregatable in all indices.
     * @param aliasesIndices         The list of indices where this field is actually an alias
     *                               or null if the field is an alias and the isAlias flag states this as-well
     * @param meta                   Merged metadata across indices.
     */
    public FieldCapabilities(
        String name,
        String type,
        boolean isAlias,
        boolean isSearchable,
        boolean isAggregatable,
        String[] aliases,
        String[] indices,
        String[] nonSearchableIndices,
        String[] nonAggregatableIndices,
        String[] aliasesIndices,
        Map<String, Set<String>> meta
    ) {
        this.name = name;
        this.type = type;
        this.isAlias = isAlias;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.aliases = aliases;
        this.indices = indices;
        this.nonSearchableIndices = nonSearchableIndices;
        this.nonAggregatableIndices = nonAggregatableIndices;
        this.aliasesIndices = aliasesIndices;
        this.meta = Objects.requireNonNull(meta);
    }

    FieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.isAlias = in.readBoolean();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        this.aliases = in.readOptionalStringArray();
        this.indices = in.readOptionalStringArray();
        this.nonSearchableIndices = in.readOptionalStringArray();
        this.nonAggregatableIndices = in.readOptionalStringArray();
        this.aliasesIndices = in.readOptionalStringArray();
        this.meta = in.readMap(StreamInput::readString, i -> i.readSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(isAlias);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        out.writeOptionalStringArray(aliases);
        out.writeOptionalStringArray(indices);
        out.writeOptionalStringArray(nonSearchableIndices);
        out.writeOptionalStringArray(nonAggregatableIndices);
        out.writeOptionalStringArray(aliasesIndices);
        out.writeMap(meta, StreamOutput::writeString, (o, set) -> o.writeCollection(set, StreamOutput::writeString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(IS_ALIAS_FIELD.getPreferredName(), isAlias);
        builder.field(SEARCHABLE_FIELD.getPreferredName(), isSearchable);
        builder.field(AGGREGATABLE_FIELD.getPreferredName(), isAggregatable);
        if (aliases != null) {
            builder.field(ALIASES_FIELD.getPreferredName(), aliases);
        }
        if (indices != null) {
            builder.field(INDICES_FIELD.getPreferredName(), indices);
        }
        if (nonSearchableIndices != null) {
            builder.field(NON_SEARCHABLE_INDICES_FIELD.getPreferredName(), nonSearchableIndices);
        }
        if (nonAggregatableIndices != null) {
            builder.field(NON_AGGREGATABLE_INDICES_FIELD.getPreferredName(), nonAggregatableIndices);
        }
        if (aliasesIndices != null) {
            builder.field(ALIAS_INDICES_FIELD.getPreferredName(), aliasesIndices);
        }
        if (meta.isEmpty() == false) {
            builder.startObject("meta");
            List<Map.Entry<String, Set<String>>> entries = new ArrayList<>(meta.entrySet());
            entries.sort(Comparator.comparing(Map.Entry::getKey)); // provide predictable order
            for (Map.Entry<String, Set<String>> entry : entries) {
                List<String> values = new ArrayList<>(entry.getValue());
                values.sort(String::compareTo); // provide predictable order
                builder.field(entry.getKey(), values);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static FieldCapabilities fromXContent(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, name);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilities, String> PARSER = new ConstructingObjectParser<>(
        "field_capabilities",
        true,
        (a, name) -> new FieldCapabilities(
            name,
            (String) a[0],
            (boolean) a[1],
            (boolean) a[2],
            (boolean) a[3],
            a[4] != null ? ((List<String>) a[4]).toArray(new String[0]) : null,
            a[5] != null ? ((List<String>) a[5]).toArray(new String[0]) : null,
            a[6] != null ? ((List<String>) a[6]).toArray(new String[0]) : null,
            a[7] != null ? ((List<String>) a[7]).toArray(new String[0]) : null,
            a[8] != null ? ((List<String>) a[8]).toArray(new String[0]) : null,
            a[9] != null ? ((Map<String, Set<String>>) a[9]) : Collections.emptyMap()
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), IS_ALIAS_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SEARCHABLE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), AGGREGATABLE_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), ALIASES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INDICES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_SEARCHABLE_INDICES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_AGGREGATABLE_INDICES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), ALIAS_INDICES_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, context) -> parser.map(HashMap::new, p -> Collections.unmodifiableSet(new HashSet<>(p.list()))),
            META_FIELD
        );
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * Whether this field is an alias to another field
     */
    public boolean isAlias() {
        return isAlias;
    }

    /**
     * Whether this field can be aggregated on all indices.
     */
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * Whether this field is indexed for search on all indices.
     */
    public boolean isSearchable() {
        return isSearchable;
    }

    /**
     * The type of the field.
     */
    public String getType() {
        return type;
    }

    /**
     * The list of indices where this field name is defined as {@code type},
     * or null if all indices have the same {@code type} for the field.
     */
    public String[] indices() {
        return indices;
    }

    /**
     * the list of aliases where this filed is referenced within (referenced by the `path` alias attribute)
     */
    public String[] aliases() {
        return aliases;
    }

    /**
     * The list of indices where this field is not searchable,
     * or null if the field is searchable in all indices.
     */
    public String[] nonSearchableIndices() {
        return nonSearchableIndices;
    }

    /**
     * The list of indices where this field is not aggregatable,
     * or null if the field is aggregatable in all indices.
     */
    public String[] nonAggregatableIndices() {
        return nonAggregatableIndices;
    }

    /**
     * The list of indices where this field is actually an alias
     */
    public String[] aliasesIndices() {
        return nonAggregatableIndices;
    }

    /**
     * Return merged metadata across indices.
     */
    public Map<String, Set<String>> meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilities that = (FieldCapabilities) o;
        return isAlias == that.isAlias
            && isSearchable == that.isSearchable
            && isAggregatable == that.isAggregatable
            && Objects.equals(name, that.name)
            && Objects.equals(type, that.type)
            && Arrays.equals(aliases, that.aliases)
            && Arrays.equals(indices, that.indices)
            && Arrays.equals(nonSearchableIndices, that.nonSearchableIndices)
            && Arrays.equals(nonAggregatableIndices, that.nonAggregatableIndices)
            && Arrays.equals(aliasesIndices, that.aliasesIndices)
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, type, isAlias, isSearchable, isAggregatable, meta);
        result = 31 * result + Arrays.hashCode(aliases);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(nonSearchableIndices);
        result = 31 * result + Arrays.hashCode(nonAggregatableIndices);
        result = 31 * result + Arrays.hashCode(aliasesIndices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Builder for field capabilities
     *
     * @opensearch.internal
     */
    static class Builder {
        private String name;
        private String type;
        private boolean isAlias;
        private boolean isSearchable;
        private boolean isAggregatable;
        private List<IndexCaps> indicesList;
        private Map<String, Set<String>> meta;

        Builder(String name, String type) {
            this.name = name;
            this.type = type;
            this.isAlias = true;
            this.isSearchable = true;
            this.isAggregatable = true;
            this.indicesList = new ArrayList<>();
            this.meta = new HashMap<>();
        }

        /**
         * Collect the field capabilities for an index.
         */
        void add(String index, boolean alias, boolean search, boolean agg, List<String> aliases, Map<String, String> meta) {
            IndexCaps indexCaps = new IndexCaps(index, aliases, search, agg, isAlias);
            indicesList.add(indexCaps);
            this.isAlias &= alias;
            this.isSearchable &= search;
            this.isAggregatable &= agg;
            for (Map.Entry<String, String> entry : meta.entrySet()) {
                this.meta.computeIfAbsent(entry.getKey(), key -> new HashSet<>()).add(entry.getValue());
            }
        }

        List<String> getIndices() {
            return indicesList.stream().map(c -> c.name).collect(Collectors.toList());
        }

        FieldCapabilities build(boolean withIndices) {
            final String[] indices;
            /* Eclipse can't deal with o -> o.name, maybe because of
             * https://bugs.eclipse.org/bugs/show_bug.cgi?id=511750 */
            Collections.sort(indicesList, Comparator.comparing((IndexCaps o) -> o.name));
            if (withIndices) {
                indices = indicesList.stream().map(caps -> caps.name).toArray(String[]::new);
            } else {
                indices = null;
            }

            List<String> aliasesCollection = indicesList.stream()
                .flatMap(caps -> caps.aliases.stream().map(alias -> String.format(Locale.ROOT, "%s:%s", caps.name, alias)))
                .collect(Collectors.toList());
            final String[] indexAliases = aliasesCollection.isEmpty() ? null : aliasesCollection.toArray(String[]::new);

            final String[] nonSearchableIndices;
            if (isSearchable == false && indicesList.stream().anyMatch((caps) -> caps.isSearchable)) {
                // Iff this field is searchable in some indices AND non-searchable in others
                // we record the list of non-searchable indices
                nonSearchableIndices = indicesList.stream()
                    .filter((caps) -> caps.isSearchable == false)
                    .map(caps -> caps.name)
                    .toArray(String[]::new);
            } else {
                nonSearchableIndices = null;
            }

            final String[] nonAggregatableIndices;
            if (isAggregatable == false && indicesList.stream().anyMatch((caps) -> caps.isAggregatable)) {
                // Iff this field is aggregatable in some indices AND non-aggregatable in others
                // we keep the list of non-aggregatable indices
                nonAggregatableIndices = indicesList.stream()
                    .filter((caps) -> caps.isAggregatable == false)
                    .map(caps -> caps.name)
                    .toArray(String[]::new);
            } else {
                nonAggregatableIndices = null;
            }
            final String[] aliasIndices;
            if (isAlias == true && indicesList.stream().anyMatch((caps) -> caps.isAlias)) {
                // If this field is alias in some indices AND non-alias in others
                // we keep the list of alias indices
                aliasIndices = indicesList.stream().filter((caps) -> caps.isAlias == true).map(caps -> caps.name).toArray(String[]::new);
            } else {
                aliasIndices = null;
            }
            final Function<Map.Entry<String, Set<String>>, Set<String>> entryValueFunction = Map.Entry::getValue;
            Map<String, Set<String>> immutableMeta = Collections.unmodifiableMap(
                meta.entrySet()
                    .stream()
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, entryValueFunction.andThen(HashSet::new).andThen(Collections::unmodifiableSet))
                    )
            );
            return new FieldCapabilities(
                name,
                type,
                isAlias,
                isSearchable,
                isAggregatable,
                indexAliases,
                indices,
                nonSearchableIndices,
                nonAggregatableIndices,
                aliasIndices,
                immutableMeta
            );
        }
    }

    /**
     * Inner index capabilities
     *
     * @opensearch.internal
     */
    private static class IndexCaps {
        final String name;
        final boolean isSearchable;
        final boolean isAggregatable;
        final boolean isAlias;
        final List<String> aliases;

        IndexCaps(String name, List<String> aliases, boolean isSearchable, boolean isAggregatable, boolean isAlias) {
            this.name = name;
            this.aliases = aliases;
            this.isSearchable = isSearchable;
            this.isAggregatable = isAggregatable;
            this.isAlias = isAlias;
        }
    }
}
