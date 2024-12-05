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

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.action.AliasesRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterState.FeatureAware;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.Diffable;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiffable;
import org.opensearch.cluster.NamedDiffableValueSerializer;
import org.opensearch.cluster.applicationtemplates.SystemTemplateMetadata;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedObjectNotFoundException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.MetadataStateFormat;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.MapperPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * Metadata information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class Metadata implements Iterable<IndexMetadata>, Diffable<Metadata>, ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(Metadata.class);

    public static final String ALL = "_all";
    public static final String UNKNOWN_CLUSTER_UUID = Strings.UNKNOWN_UUID_VALUE;
    public static final Pattern NUMBER_PATTERN = Pattern.compile("[0-9]+$");

    /**
     * Utility to identify whether input index uses SEGMENT replication strategy in established cluster state metadata.
     * Note: Method intended for use by other plugins as well.
     *
     * @param indexName Index name
     * @return true if index uses SEGMENT replication, false otherwise
     */
    public boolean isSegmentReplicationEnabled(String indexName) {
        return Optional.ofNullable(index(indexName))
            .map(
                indexMetadata -> ReplicationType.parseString(indexMetadata.getSettings().get(IndexMetadata.SETTING_REPLICATION_TYPE))
                    .equals(ReplicationType.SEGMENT)
            )
            .orElse(false);
    }

    /**
     * Context of the XContent.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum XContentContext {
        /* Custom metadata should be returns as part of API call */
        API,

        /* Custom metadata should be stored as part of the persistent cluster state */
        GATEWAY,

        /* Custom metadata should be stored as part of a snapshot */
        SNAPSHOT
    }

    /**
     * Indicates that this custom metadata will be returned as part of an API call but will not be persisted
     */
    public static EnumSet<XContentContext> API_ONLY = EnumSet.of(XContentContext.API);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and will be persisted between
     * node restarts, but will not be a part of a snapshot global state
     */
    public static EnumSet<XContentContext> API_AND_GATEWAY = EnumSet.of(XContentContext.API, XContentContext.GATEWAY);

    /**
     * Indicates that this custom metadata will be returned as part of an API call and stored as a part of
     * a snapshot global state, but will not be persisted between node restarts
     */
    public static EnumSet<XContentContext> API_AND_SNAPSHOT = EnumSet.of(XContentContext.API, XContentContext.SNAPSHOT);

    /**
     * Indicates that this custom metadata will be returned as part of an API call, stored as a part of
     * a snapshot global state, and will be persisted between node restarts
     */
    public static EnumSet<XContentContext> ALL_CONTEXTS = EnumSet.allOf(XContentContext.class);

    /**
     * Custom metadata.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment, ClusterState.FeatureAware {

        EnumSet<XContentContext> context();

        static Custom fromXContent(XContentParser parser, String name) throws IOException {
            // handling any Exception is caller's responsibility
            return parser.namedObject(Custom.class, name, null);
        }
    }

    public static final Setting<Integer> DEFAULT_REPLICA_COUNT_SETTING = Setting.intSetting(
        "cluster.default_number_of_replicas",
        1,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> SETTING_READ_ONLY_SETTING = Setting.boolSetting(
        "cluster.blocks.read_only",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final ClusterBlock CLUSTER_READ_ONLY_BLOCK = new ClusterBlock(
        6,
        "cluster read-only (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );

    public static final ClusterBlock CLUSTER_CREATE_INDEX_BLOCK = new ClusterBlock(
        10,
        "cluster create-index blocked (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.CREATE_INDEX)
    );

    public static final Setting<Boolean> SETTING_READ_ONLY_ALLOW_DELETE_SETTING = Setting.boolSetting(
        "cluster.blocks.read_only_allow_delete",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> SETTING_CREATE_INDEX_BLOCK_SETTING = Setting.boolSetting(
        "cluster.blocks.create_index",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final ClusterBlock CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(
        13,
        "cluster read-only / allow delete (api)",
        false,
        false,
        true,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );

    public static final Metadata EMPTY_METADATA = builder().build();

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String CONTEXT_MODE_SNAPSHOT = XContentContext.SNAPSHOT.toString();

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String CONTEXT_MODE_API = XContentContext.API.toString();

    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";

    public static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    private final long version;

    private final CoordinationMetadata coordinationMetadata;

    private final Settings transientSettings;
    private final Settings persistentSettings;
    private final Settings settings;
    private final DiffableStringMap hashesOfConsistentSettings;
    private final Map<String, IndexMetadata> indices;
    private final TemplatesMetadata templates;
    private final Map<String, Custom> customs;

    private final transient int totalNumberOfShards; // Transient ? not serializable anyway?
    private final int totalOpenIndexShards;

    private final String[] allIndices;
    private final String[] visibleIndices;
    private final String[] allOpenIndices;
    private final String[] visibleOpenIndices;
    private final String[] allClosedIndices;
    private final String[] visibleClosedIndices;

    private final SortedMap<String, IndexAbstraction> indicesLookup;

    private final Map<String, SortedMap<Long, String>> systemTemplatesLookup;

    Metadata(
        String clusterUUID,
        boolean clusterUUIDCommitted,
        long version,
        CoordinationMetadata coordinationMetadata,
        Settings transientSettings,
        Settings persistentSettings,
        DiffableStringMap hashesOfConsistentSettings,
        final Map<String, IndexMetadata> indices,
        final Map<String, IndexTemplateMetadata> templates,
        final Map<String, Custom> customs,
        String[] allIndices,
        String[] visibleIndices,
        String[] allOpenIndices,
        String[] visibleOpenIndices,
        String[] allClosedIndices,
        String[] visibleClosedIndices,
        SortedMap<String, IndexAbstraction> indicesLookup,
        Map<String, SortedMap<Long, String>> systemTemplatesLookup
    ) {
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
        this.version = version;
        this.coordinationMetadata = coordinationMetadata;
        this.transientSettings = transientSettings;
        this.persistentSettings = persistentSettings;
        this.settings = Settings.builder().put(persistentSettings).put(transientSettings).build();
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
        this.indices = Collections.unmodifiableMap(indices);
        this.customs = Collections.unmodifiableMap(customs);
        this.templates = new TemplatesMetadata(templates);
        int totalNumberOfShards = 0;
        int totalOpenIndexShards = 0;
        for (IndexMetadata cursor : indices.values()) {
            totalNumberOfShards += cursor.getTotalNumberOfShards();
            if (IndexMetadata.State.OPEN.equals(cursor.getState())) {
                totalOpenIndexShards += cursor.getTotalNumberOfShards();
            }
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.totalOpenIndexShards = totalOpenIndexShards;

        this.allIndices = allIndices;
        this.visibleIndices = visibleIndices;
        this.allOpenIndices = allOpenIndices;
        this.visibleOpenIndices = visibleOpenIndices;
        this.allClosedIndices = allClosedIndices;
        this.visibleClosedIndices = visibleClosedIndices;
        this.indicesLookup = indicesLookup;
        this.systemTemplatesLookup = systemTemplatesLookup;
    }

    public long version() {
        return this.version;
    }

    public String clusterUUID() {
        return this.clusterUUID;
    }

    /**
     * Whether the current node with the given cluster state is locked into the cluster with the UUID returned by {@link #clusterUUID()},
     * meaning that it will not accept any cluster state with a different clusterUUID.
     */
    public boolean clusterUUIDCommitted() {
        return this.clusterUUIDCommitted;
    }

    /**
     * Returns the merged transient and persistent settings.
     */
    public Settings settings() {
        return this.settings;
    }

    public Settings transientSettings() {
        return this.transientSettings;
    }

    public Settings persistentSettings() {
        return this.persistentSettings;
    }

    public Map<String, String> hashesOfConsistentSettings() {
        return this.hashesOfConsistentSettings;
    }

    public CoordinationMetadata coordinationMetadata() {
        return this.coordinationMetadata;
    }

    public boolean hasAlias(String alias) {
        IndexAbstraction indexAbstraction = getIndicesLookup().get(alias);
        if (indexAbstraction != null) {
            return indexAbstraction.getType() == IndexAbstraction.Type.ALIAS;
        } else {
            return false;
        }
    }

    public boolean equalsAliases(Metadata other) {
        for (IndexMetadata otherIndex : other.indices().values()) {
            IndexMetadata thisIndex = index(otherIndex.getIndex());
            if (thisIndex == null) {
                return false;
            }
            if (otherIndex.getAliases().equals(thisIndex.getAliases()) == false) {
                return false;
            }
        }

        return true;
    }

    public SortedMap<String, IndexAbstraction> getIndicesLookup() {
        return indicesLookup;
    }

    /**
     * Finds the specific index aliases that point to the requested concrete indices directly
     * or that match with the indices via wildcards.
     *
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned.
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public Map<String, List<AliasMetadata>> findAllAliases(final String[] concreteIndices) {
        return findAliases(Strings.EMPTY_ARRAY, concreteIndices);
    }

    /**
     * Finds the specific index aliases that match with the specified aliases directly or partially via wildcards, and
     * that point to the specified concrete indices (directly or matching indices via wildcards).
     *
     * @param aliasesRequest The request to find aliases for
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned.
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    public Map<String, List<AliasMetadata>> findAliases(final AliasesRequest aliasesRequest, final String[] concreteIndices) {
        return findAliases(aliasesRequest.aliases(), concreteIndices);
    }

    /**
     * Finds the specific index aliases that match with the specified aliases directly or partially via wildcards, and
     * that point to the specified concrete indices (directly or matching indices via wildcards).
     *
     * @param aliases The aliases to look for. Might contain include or exclude wildcards.
     * @param concreteIndices The concrete indices that the aliases must point to in order to be returned
     * @return A map of index name to the list of aliases metadata. If a concrete index does not have matching
     * aliases then the result will <b>not</b> include the index's key.
     */
    private Map<String, List<AliasMetadata>> findAliases(final String[] aliases, final String[] concreteIndices) {
        assert aliases != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return Map.of();
        }
        String[] patterns = new String[aliases.length];
        boolean[] include = new boolean[aliases.length];
        for (int i = 0; i < aliases.length; i++) {
            String alias = aliases[i];
            if (alias.charAt(0) == '-') {
                patterns[i] = alias.substring(1);
                include[i] = false;
            } else {
                patterns[i] = alias;
                include[i] = true;
            }
        }
        boolean matchAllAliases = patterns.length == 0;
        final Map<String, List<AliasMetadata>> mapBuilder = new HashMap<>();
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = indices.get(index);
            List<AliasMetadata> filteredValues = new ArrayList<>();
            for (final AliasMetadata value : indexMetadata.getAliases().values()) {
                boolean matched = matchAllAliases;
                String alias = value.alias();
                for (int i = 0; i < patterns.length; i++) {
                    if (include[i]) {
                        if (matched == false) {
                            String pattern = patterns[i];
                            matched = ALL.equals(pattern) || Regex.simpleMatch(pattern, alias);
                        }
                    } else if (matched) {
                        matched = Regex.simpleMatch(patterns[i], alias) == false;
                    }
                }
                if (matched) {
                    filteredValues.add(value);
                }
            }
            if (filteredValues.isEmpty() == false) {
                // Make the list order deterministic
                CollectionUtil.timSort(filteredValues, Comparator.comparing(AliasMetadata::alias));
                mapBuilder.put(index, Collections.unmodifiableList(filteredValues));
            }
        }
        return mapBuilder;
    }

    /**
     * Finds all mappings for concrete indices. Only fields that match the provided field
     * filter will be returned (default is a predicate that always returns true, which can be
     * overridden via plugins)
     *
     * @see MapperPlugin#getFieldFilter()
     *
     */
    public Map<String, MappingMetadata> findMappings(String[] concreteIndices, Function<String, Predicate<String>> fieldFilter)
        throws IOException {
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return Map.of();
        }

        final Map<String, MappingMetadata> indexMapBuilder = new HashMap<>();
        Arrays.stream(concreteIndices)
            .filter(indices.keySet()::contains)
            .forEach((idx) -> indexMapBuilder.put(idx, filterFields(indices.get(idx).mapping(), fieldFilter.apply(idx))));

        return Collections.unmodifiableMap(indexMapBuilder);
    }

    /**
     * Finds the parent data streams, if any, for the specified concrete indices.
     */
    public Map<String, IndexAbstraction.DataStream> findDataStreams(String[] concreteIndices) {
        assert concreteIndices != null;
        final Map<String, IndexAbstraction.DataStream> builder = new HashMap<>();
        final SortedMap<String, IndexAbstraction> lookup = getIndicesLookup();
        for (String indexName : concreteIndices) {
            IndexAbstraction index = lookup.get(indexName);
            assert index != null;
            assert index.getType() == IndexAbstraction.Type.CONCRETE_INDEX;
            if (index.getParentDataStream() != null) {
                builder.put(indexName, index.getParentDataStream());
            }
        }
        return Collections.unmodifiableMap(builder);
    }

    @SuppressWarnings("unchecked")
    private static MappingMetadata filterFields(MappingMetadata mappingMetadata, Predicate<String> fieldPredicate) {
        if (mappingMetadata == null) {
            return MappingMetadata.EMPTY_MAPPINGS;
        }
        if (fieldPredicate == MapperPlugin.NOOP_FIELD_PREDICATE) {
            return mappingMetadata;
        }
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(mappingMetadata.source().compressedReference(), true).v2();
        Map<String, Object> mapping;
        if (sourceAsMap.size() == 1 && sourceAsMap.containsKey(mappingMetadata.type())) {
            mapping = (Map<String, Object>) sourceAsMap.get(mappingMetadata.type());
        } else {
            mapping = sourceAsMap;
        }

        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        if (properties == null || properties.isEmpty()) {
            return mappingMetadata;
        }

        filterFields("", properties, fieldPredicate);

        return new MappingMetadata(mappingMetadata.type(), sourceAsMap);
    }

    @SuppressWarnings("unchecked")
    private static boolean filterFields(String currentPath, Map<String, Object> fields, Predicate<String> fieldPredicate) {
        assert fieldPredicate != MapperPlugin.NOOP_FIELD_PREDICATE;
        Iterator<Map.Entry<String, Object>> entryIterator = fields.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, Object> entry = entryIterator.next();
            String newPath = mergePaths(currentPath, entry.getKey());
            Object value = entry.getValue();
            boolean mayRemove = true;
            boolean isMultiField = false;
            if (value instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) value;
                Map<String, Object> properties = (Map<String, Object>) map.get("properties");
                if (properties != null) {
                    mayRemove = filterFields(newPath, properties, fieldPredicate);
                } else {
                    Map<String, Object> subFields = (Map<String, Object>) map.get("fields");
                    if (subFields != null) {
                        isMultiField = true;
                        if (mayRemove = filterFields(newPath, subFields, fieldPredicate)) {
                            map.remove("fields");
                        }
                    }
                }
            } else {
                throw new IllegalStateException("cannot filter mappings, found unknown element of type [" + value.getClass() + "]");
            }

            // only remove a field if it has no sub-fields left and it has to be excluded
            if (fieldPredicate.test(newPath) == false) {
                if (mayRemove) {
                    entryIterator.remove();
                } else if (isMultiField) {
                    // multi fields that should be excluded but hold subfields that don't have to be excluded are converted to objects
                    Map<String, Object> map = (Map<String, Object>) value;
                    Map<String, Object> subFields = (Map<String, Object>) map.get("fields");
                    assert subFields.size() > 0;
                    map.put("properties", subFields);
                    map.remove("fields");
                    map.remove("type");
                }
            }
        }
        // return true if the ancestor may be removed, as it has no sub-fields left
        return fields.size() == 0;
    }

    private static String mergePaths(String path, String field) {
        if (path.length() == 0) {
            return field;
        }
        return path + "." + field;
    }

    /**
     * Returns all the concrete indices.
     */
    public String[] getConcreteAllIndices() {
        return allIndices;
    }

    /**
     * Returns all the concrete indices that are not hidden.
     */
    public String[] getConcreteVisibleIndices() {
        return visibleIndices;
    }

    /**
     * Returns all of the concrete indices that are open.
     */
    public String[] getConcreteAllOpenIndices() {
        return allOpenIndices;
    }

    /**
     * Returns all of the concrete indices that are open and not hidden.
     */
    public String[] getConcreteVisibleOpenIndices() {
        return visibleOpenIndices;
    }

    /**
     * Returns all of the concrete indices that are closed.
     */
    public String[] getConcreteAllClosedIndices() {
        return allClosedIndices;
    }

    /**
     * Returns all of the concrete indices that are closed and not hidden.
     */
    public String[] getConcreteVisibleClosedIndices() {
        return visibleClosedIndices;
    }

    /**
     * Returns indexing routing for the given <code>aliasOrIndex</code>. Resolves routing from the alias metadata used
     * in the write index.
     */
    public String resolveWriteIndexRouting(@Nullable String routing, String aliasOrIndex) {
        if (aliasOrIndex == null) {
            return routing;
        }

        IndexAbstraction result = getIndicesLookup().get(aliasOrIndex);
        if (result == null || result.getType() != IndexAbstraction.Type.ALIAS) {
            return routing;
        }
        IndexMetadata writeIndex = result.getWriteIndex();
        if (writeIndex == null) {
            throw new IllegalArgumentException("alias [" + aliasOrIndex + "] does not have a write index");
        }
        AliasMetadata aliasMd = writeIndex.getAliases().get(result.getName());
        if (aliasMd.indexRouting() != null) {
            if (aliasMd.indexRouting().indexOf(',') != -1) {
                throw new IllegalArgumentException(
                    "index/alias ["
                        + aliasOrIndex
                        + "] provided with routing value ["
                        + aliasMd.getIndexRouting()
                        + "] that resolved to several routing values, rejecting operation"
                );
            }
            if (routing != null) {
                if (!routing.equals(aliasMd.indexRouting())) {
                    throw new IllegalArgumentException(
                        "Alias ["
                            + aliasOrIndex
                            + "] has index routing associated with it ["
                            + aliasMd.indexRouting()
                            + "], and was provided with routing value ["
                            + routing
                            + "], rejecting operation"
                    );
                }
            }
            // Alias routing overrides the parent routing (if any).
            return aliasMd.indexRouting();
        }
        return routing;
    }

    /**
     * Returns indexing routing for the given index.
     */
    // TODO: This can be moved to IndexNameExpressionResolver too, but this means that we will support wildcards and other expressions
    // in the index,bulk,update and delete apis.
    public String resolveIndexRouting(@Nullable String routing, String aliasOrIndex) {
        if (aliasOrIndex == null) {
            return routing;
        }

        IndexAbstraction result = getIndicesLookup().get(aliasOrIndex);
        if (result == null || result.getType() != IndexAbstraction.Type.ALIAS) {
            return routing;
        }
        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) result;
        if (result.getIndices().size() > 1) {
            rejectSingleIndexOperation(aliasOrIndex, result);
        }
        AliasMetadata aliasMd = alias.getFirstAliasMetadata();
        if (aliasMd.indexRouting() != null) {
            if (aliasMd.indexRouting().indexOf(',') != -1) {
                throw new IllegalArgumentException(
                    "index/alias ["
                        + aliasOrIndex
                        + "] provided with routing value ["
                        + aliasMd.getIndexRouting()
                        + "] that resolved to several routing values, rejecting operation"
                );
            }
            if (routing != null) {
                if (!routing.equals(aliasMd.indexRouting())) {
                    throw new IllegalArgumentException(
                        "Alias ["
                            + aliasOrIndex
                            + "] has index routing associated with it ["
                            + aliasMd.indexRouting()
                            + "], and was provided with routing value ["
                            + routing
                            + "], rejecting operation"
                    );
                }
            }
            // Alias routing overrides the parent routing (if any).
            return aliasMd.indexRouting();
        }
        return routing;
    }

    private void rejectSingleIndexOperation(String aliasOrIndex, IndexAbstraction result) {
        String[] indexNames = new String[result.getIndices().size()];
        int i = 0;
        for (IndexMetadata indexMetadata : result.getIndices()) {
            indexNames[i++] = indexMetadata.getIndex().getName();
        }
        throw new IllegalArgumentException(
            "Alias ["
                + aliasOrIndex
                + "] has more than one index associated with it ["
                + Arrays.toString(indexNames)
                + "], can't execute a single index op"
        );
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public boolean hasIndex(Index index) {
        IndexMetadata metadata = index(index.getName());
        return metadata != null && metadata.getIndexUUID().equals(index.getUUID());
    }

    public boolean hasConcreteIndex(String index) {
        return getIndicesLookup().containsKey(index);
    }

    public IndexMetadata index(String index) {
        return indices.get(index);
    }

    public IndexMetadata index(Index index) {
        IndexMetadata metadata = index(index.getName());
        if (metadata != null && metadata.getIndexUUID().equals(index.getUUID())) {
            return metadata;
        }
        return null;
    }

    /** Returns true iff existing index has the same {@link IndexMetadata} instance */
    public boolean hasIndexMetadata(final IndexMetadata indexMetadata) {
        return indices.get(indexMetadata.getIndex().getName()) == indexMetadata;
    }

    /**
     * Returns the {@link IndexMetadata} for this index.
     * @throws IndexNotFoundException if no metadata for this index is found
     */
    public IndexMetadata getIndexSafe(Index index) {
        IndexMetadata metadata = index(index.getName());
        if (metadata != null) {
            if (metadata.getIndexUUID().equals(index.getUUID())) {
                return metadata;
            }
            throw new IndexNotFoundException(
                index,
                new IllegalStateException(
                    "index uuid doesn't match expected: [" + index.getUUID() + "] but got: [" + metadata.getIndexUUID() + "]"
                )
            );
        }
        throw new IndexNotFoundException(index);
    }

    public Map<String, IndexMetadata> indices() {
        return this.indices;
    }

    public Map<String, IndexMetadata> getIndices() {
        return indices();
    }

    public Map<String, IndexTemplateMetadata> templates() {
        return this.templates.getTemplates();
    }

    public Map<String, IndexTemplateMetadata> getTemplates() {
        return templates();
    }

    public TemplatesMetadata templatesMetadata() {
        return this.templates;
    }

    public Map<String, ComponentTemplate> componentTemplates() {
        return Optional.ofNullable((ComponentTemplateMetadata) this.custom(ComponentTemplateMetadata.TYPE))
            .map(ComponentTemplateMetadata::componentTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, SortedMap<Long, String>> systemTemplatesLookup() {
        return systemTemplatesLookup;
    }

    public Map<String, ComposableIndexTemplate> templatesV2() {
        return Optional.ofNullable((ComposableIndexTemplateMetadata) this.custom(ComposableIndexTemplateMetadata.TYPE))
            .map(ComposableIndexTemplateMetadata::indexTemplates)
            .orElse(Collections.emptyMap());
    }

    public Map<String, DataStream> dataStreams() {
        return Optional.ofNullable((DataStreamMetadata) this.custom(DataStreamMetadata.TYPE))
            .map(DataStreamMetadata::dataStreams)
            .orElse(Collections.emptyMap());
    }

    public Map<String, View> views() {
        return Optional.ofNullable((ViewMetadata) this.custom(ViewMetadata.TYPE)).map(ViewMetadata::views).orElse(Collections.emptyMap());
    }

    public Map<String, QueryGroup> queryGroups() {
        return Optional.ofNullable((QueryGroupMetadata) this.custom(QueryGroupMetadata.TYPE))
            .map(QueryGroupMetadata::queryGroups)
            .orElse(Collections.emptyMap());
    }

    public DecommissionAttributeMetadata decommissionAttributeMetadata() {
        return custom(DecommissionAttributeMetadata.TYPE);
    }

    public Map<String, Custom> customs() {
        return this.customs;
    }

    public Map<String, Custom> getCustoms() {
        return this.customs();
    }

    /**
     * The collection of index deletions in the cluster.
     */
    public IndexGraveyard indexGraveyard() {
        return custom(IndexGraveyard.TYPE);
    }

    /**
     * *
     * @return The weighted routing metadata for search requests
     */
    public WeightedRoutingMetadata weightedRoutingMetadata() {
        return custom(WeightedRoutingMetadata.TYPE);
    }

    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    /**
     * Gets the total number of shards from all indices, including replicas and
     * closed indices.
     * @return The total number shards from all indices.
     */
    public int getTotalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    /**
     * Gets the total number of open shards from all indices. Includes
     * replicas, but does not include shards that are part of closed indices.
     * @return The total number of open shards from all indices.
     */
    public int getTotalOpenIndexShards() {
        return this.totalOpenIndexShards;
    }

    /**
     * Identifies whether the array containing type names given as argument refers to all types
     * The empty or null array identifies all types
     *
     * @param types the array containing types
     * @return true if the provided array maps to all types, false otherwise
     */
    public static boolean isAllTypes(String[] types) {
        return types == null || types.length == 0 || isExplicitAllType(types);
    }

    /**
     * Identifies whether the array containing type names given as argument explicitly refers to all types
     * The empty or null array doesn't explicitly map to all types
     *
     * @param types the array containing index names
     * @return true if the provided array explicitly maps to all types, false otherwise
     */
    public static boolean isExplicitAllType(String[] types) {
        return types != null && types.length == 1 && ALL.equals(types[0]);
    }

    /**
     * @param concreteIndex The concrete index to check if routing is required
     * @return Whether routing is required according to the mapping for the specified index and type
     */
    public boolean routingRequired(String concreteIndex) {
        IndexMetadata indexMetadata = indices.get(concreteIndex);
        if (indexMetadata != null) {
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                return mappingMetadata.routingRequired();
            }
        }
        return false;
    }

    @Override
    public Iterator<IndexMetadata> iterator() {
        return indices.values().iterator();
    }

    public static boolean isGlobalStateEquals(Metadata metadata1, Metadata metadata2) {
        if (!isCoordinationMetadataEqual(metadata1, metadata2)) {
            return false;
        }
        if (!metadata1.hashesOfConsistentSettings.equals(metadata2.hashesOfConsistentSettings)) {
            return false;
        }
        if (!metadata1.clusterUUID.equals(metadata2.clusterUUID)) {
            return false;
        }
        if (metadata1.clusterUUIDCommitted != metadata2.clusterUUIDCommitted) {
            return false;
        }
        return isGlobalResourcesMetadataEquals(metadata1, metadata2);
    }

    /**
     * Compares Metadata entities persisted in Remote Store.
     */
    public static boolean isGlobalResourcesMetadataEquals(Metadata metadata1, Metadata metadata2) {
        if (!isSettingsMetadataEqual(metadata1, metadata2)) {
            return false;
        }
        if (!isTemplatesMetadataEqual(metadata1, metadata2)) {
            return false;
        }
        // Check if any persistent metadata needs to be saved
        return isCustomMetadataEqual(metadata1, metadata2);
    }

    public static boolean isCoordinationMetadataEqual(Metadata metadata1, Metadata metadata2) {
        return metadata1.coordinationMetadata.equals(metadata2.coordinationMetadata);
    }

    public static boolean isSettingsMetadataEqual(Metadata metadata1, Metadata metadata2) {
        return metadata1.persistentSettings.equals(metadata2.persistentSettings);
    }

    public static boolean isTransientSettingsMetadataEqual(Metadata metadata1, Metadata metadata2) {
        return metadata1.transientSettings.equals(metadata2.transientSettings);
    }

    public static boolean isTemplatesMetadataEqual(Metadata metadata1, Metadata metadata2) {
        return metadata1.templates.equals(metadata2.templates);
    }

    public static boolean isHashesOfConsistentSettingsEqual(Metadata metadata1, Metadata metadata2) {
        return metadata1.hashesOfConsistentSettings.equals(metadata2.hashesOfConsistentSettings);
    }

    public static boolean isCustomMetadataEqual(Metadata metadata1, Metadata metadata2) {
        int customCount1 = 0;
        for (Map.Entry<String, Custom> cursor : metadata1.customs.entrySet()) {
            if (cursor.getValue().context().contains(XContentContext.GATEWAY)) {
                if (!cursor.getValue().equals(metadata2.custom(cursor.getKey()))) return false;
                customCount1++;
            }
        }
        int customCount2 = 0;
        for (final Custom cursor : metadata2.customs.values()) {
            if (cursor.context().contains(XContentContext.GATEWAY)) {
                customCount2++;
            }
        }
        return customCount1 == customCount2;
    }

    @Override
    public Diff<Metadata> diff(Metadata previousState) {
        return new MetadataDiff(previousState, this);
    }

    public static Diff<Metadata> readDiffFrom(StreamInput in) throws IOException {
        return new MetadataDiff(in);
    }

    public static Metadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    /**
     * A diff of metadata.
     *
     * @opensearch.internal
     */
    private static class MetadataDiff implements Diff<Metadata> {

        private final long version;
        private final String clusterUUID;
        private boolean clusterUUIDCommitted;
        private final CoordinationMetadata coordinationMetadata;
        private final Settings transientSettings;
        private final Settings persistentSettings;
        private final Diff<DiffableStringMap> hashesOfConsistentSettings;
        private final Diff<Map<String, IndexMetadata>> indices;
        private final Diff<Map<String, IndexTemplateMetadata>> templates;
        private final Diff<Map<String, Custom>> customs;

        MetadataDiff(Metadata before, Metadata after) {
            clusterUUID = after.clusterUUID;
            clusterUUIDCommitted = after.clusterUUIDCommitted;
            version = after.version;
            coordinationMetadata = after.coordinationMetadata;
            transientSettings = after.transientSettings;
            persistentSettings = after.persistentSettings;
            hashesOfConsistentSettings = after.hashesOfConsistentSettings.diff(before.hashesOfConsistentSettings);
            indices = DiffableUtils.diff(before.indices, after.indices, DiffableUtils.getStringKeySerializer());
            templates = DiffableUtils.diff(
                before.templates.getTemplates(),
                after.templates.getTemplates(),
                DiffableUtils.getStringKeySerializer()
            );
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        private static final DiffableUtils.DiffableValueReader<String, IndexMetadata> INDEX_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexMetadata::readFrom, IndexMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, IndexTemplateMetadata> TEMPLATES_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexTemplateMetadata::readFrom, IndexTemplateMetadata::readDiffFrom);

        MetadataDiff(StreamInput in) throws IOException {
            clusterUUID = in.readString();
            clusterUUIDCommitted = in.readBoolean();
            version = in.readLong();
            coordinationMetadata = new CoordinationMetadata(in);
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            hashesOfConsistentSettings = DiffableStringMap.readDiffFrom(in);
            indices = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), INDEX_METADATA_DIFF_VALUE_READER);
            templates = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), TEMPLATES_DIFF_VALUE_READER);
            customs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterUUID);
            out.writeBoolean(clusterUUIDCommitted);
            out.writeLong(version);
            coordinationMetadata.writeTo(out);
            Settings.writeSettingsToStream(transientSettings, out);
            Settings.writeSettingsToStream(persistentSettings, out);
            hashesOfConsistentSettings.writeTo(out);
            indices.writeTo(out);
            templates.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public Metadata apply(Metadata part) {
            Builder builder = builder();
            builder.clusterUUID(clusterUUID);
            builder.clusterUUIDCommitted(clusterUUIDCommitted);
            builder.version(version);
            builder.coordinationMetadata(coordinationMetadata);
            builder.transientSettings(transientSettings);
            builder.persistentSettings(persistentSettings);
            builder.hashesOfConsistentSettings(hashesOfConsistentSettings.apply(part.hashesOfConsistentSettings));
            builder.indices(indices.apply(part.indices));
            builder.templates(templates.apply(part.templates.getTemplates()));
            builder.customs(customs.apply(part.customs));
            return builder.build();
        }
    }

    public static Metadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder();
        builder.version = in.readLong();
        builder.clusterUUID = in.readString();
        builder.clusterUUIDCommitted = in.readBoolean();
        builder.coordinationMetadata(new CoordinationMetadata(in));
        builder.transientSettings(readSettingsFromStream(in));
        builder.persistentSettings(readSettingsFromStream(in));
        builder.hashesOfConsistentSettings(DiffableStringMap.readFrom(in));
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexMetadata.readFrom(in), false);
        }
        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(IndexTemplateMetadata.readFrom(in));
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeString(clusterUUID);
        out.writeBoolean(clusterUUIDCommitted);
        coordinationMetadata.writeTo(out);
        writeSettingsToStream(transientSettings, out);
        writeSettingsToStream(persistentSettings, out);
        hashesOfConsistentSettings.writeTo(out);
        out.writeVInt(indices.size());
        for (IndexMetadata indexMetadata : this) {
            indexMetadata.writeTo(out);
        }
        templates.writeTo(out);
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (final Custom cursor : customs.values()) {
            if (FeatureAware.shouldSerialize(out, cursor)) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (final Custom cursor : customs.values()) {
            if (FeatureAware.shouldSerialize(out, cursor)) {
                out.writeNamedWriteable(cursor);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Metadata metadata) {
        return new Builder(metadata);
    }

    /**
     * Builder of metadata.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private long version;

        private CoordinationMetadata coordinationMetadata = CoordinationMetadata.EMPTY_METADATA;
        private Settings transientSettings = Settings.Builder.EMPTY_SETTINGS;
        private Settings persistentSettings = Settings.Builder.EMPTY_SETTINGS;
        private DiffableStringMap hashesOfConsistentSettings = new DiffableStringMap(Collections.emptyMap());

        private final Map<String, IndexMetadata> indices;
        private final Map<String, IndexTemplateMetadata> templates;
        private final Map<String, Custom> customs;
        private final Metadata previousMetadata;

        private Map<String, SortedMap<Long, String>> systemTemplatesLookup;

        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
            indices = new HashMap<>();
            templates = new HashMap<>();
            customs = new HashMap<>();
            previousMetadata = null;
            indexGraveyard(IndexGraveyard.builder().build()); // create new empty index graveyard to initialize
        }

        public Builder(Metadata metadata) {
            this.clusterUUID = metadata.clusterUUID;
            this.clusterUUIDCommitted = metadata.clusterUUIDCommitted;
            this.coordinationMetadata = metadata.coordinationMetadata;
            this.transientSettings = metadata.transientSettings;
            this.persistentSettings = metadata.persistentSettings;
            this.hashesOfConsistentSettings = metadata.hashesOfConsistentSettings;
            this.version = metadata.version;
            this.indices = new HashMap<>(metadata.indices);
            this.templates = new HashMap<>(metadata.templates.getTemplates());
            this.customs = new HashMap<>(metadata.customs);
            this.previousMetadata = metadata;
        }

        public Builder put(IndexMetadata.Builder indexMetadataBuilder) {
            // we know its a new one, increment the version and store
            indexMetadataBuilder.version(indexMetadataBuilder.version() + 1);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public Builder put(IndexMetadata indexMetadata, boolean incrementVersion) {
            if (indices.get(indexMetadata.getIndex().getName()) == indexMetadata) {
                return this;
            }
            // if we put a new index metadata, increment its version
            if (incrementVersion) {
                indexMetadata = IndexMetadata.builder(indexMetadata).version(indexMetadata.getVersion() + 1).build();
            }
            indices.put(indexMetadata.getIndex().getName(), indexMetadata);
            return this;
        }

        public IndexMetadata get(String index) {
            return indices.get(index);
        }

        public IndexMetadata getSafe(Index index) {
            IndexMetadata indexMetadata = get(index.getName());
            if (indexMetadata != null) {
                if (indexMetadata.getIndexUUID().equals(index.getUUID())) {
                    return indexMetadata;
                }
                throw new IndexNotFoundException(
                    index,
                    new IllegalStateException(
                        "index uuid doesn't match expected: [" + index.getUUID() + "] but got: [" + indexMetadata.getIndexUUID() + "]"
                    )
                );
            }
            throw new IndexNotFoundException(index);
        }

        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        public Builder removeAllIndices() {
            indices.clear();
            return this;
        }

        public Builder indices(final Map<String, IndexMetadata> indices) {
            this.indices.putAll(indices);
            return this;
        }

        public Builder put(IndexTemplateMetadata.Builder template) {
            return put(template.build());
        }

        public Builder put(IndexTemplateMetadata template) {
            templates.put(template.name(), template);
            return this;
        }

        public Builder removeTemplate(String templateName) {
            templates.remove(templateName);
            return this;
        }

        public Builder templates(Map<String, IndexTemplateMetadata> templates) {
            this.templates.putAll(templates);
            return this;
        }

        public Builder templates(TemplatesMetadata templatesMetadata) {
            this.templates.clear();
            this.templates.putAll(templatesMetadata.getTemplates());
            return this;
        }

        public Builder put(String name, ComponentTemplate componentTemplate) {
            Objects.requireNonNull(componentTemplate, "it is invalid to add a null component template: " + name);
            Map<String, ComponentTemplate> existingTemplates = Optional.ofNullable(
                (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE)
            ).map(ctm -> new HashMap<>(ctm.componentTemplates())).orElse(new HashMap<>());
            existingTemplates.put(name, componentTemplate);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeComponentTemplate(String name) {
            Map<String, ComponentTemplate> existingTemplates = Optional.ofNullable(
                (ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE)
            ).map(ctm -> new HashMap<>(ctm.componentTemplates())).orElse(new HashMap<>());
            existingTemplates.remove(name);
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder componentTemplates(Map<String, ComponentTemplate> componentTemplates) {
            this.customs.put(ComponentTemplateMetadata.TYPE, new ComponentTemplateMetadata(componentTemplates));
            return this;
        }

        public Builder indexTemplates(Map<String, ComposableIndexTemplate> indexTemplates) {
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(indexTemplates));
            return this;
        }

        public Builder put(String name, ComposableIndexTemplate indexTemplate) {
            Objects.requireNonNull(indexTemplate, "it is invalid to add a null index template: " + name);
            Map<String, ComposableIndexTemplate> existingTemplates = Optional.ofNullable(
                (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE)
            ).map(itmd -> new HashMap<>(itmd.indexTemplates())).orElse(new HashMap<>());
            existingTemplates.put(name, indexTemplate);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public Builder removeIndexTemplate(String name) {
            Map<String, ComposableIndexTemplate> existingTemplates = Optional.ofNullable(
                (ComposableIndexTemplateMetadata) this.customs.get(ComposableIndexTemplateMetadata.TYPE)
            ).map(itmd -> new HashMap<>(itmd.indexTemplates())).orElse(new HashMap<>());
            existingTemplates.remove(name);
            this.customs.put(ComposableIndexTemplateMetadata.TYPE, new ComposableIndexTemplateMetadata(existingTemplates));
            return this;
        }

        public DataStream dataStream(String dataStreamName) {
            return ((DataStreamMetadata) customs.get(DataStreamMetadata.TYPE)).dataStreams().get(dataStreamName);
        }

        public Builder dataStreams(Map<String, DataStream> dataStreams) {
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(dataStreams));
            return this;
        }

        public Builder put(DataStream dataStream) {
            Objects.requireNonNull(dataStream, "it is invalid to add a null data stream");
            Map<String, DataStream> existingDataStreams = Optional.ofNullable(
                (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE)
            ).map(dsmd -> new HashMap<>(dsmd.dataStreams())).orElse(new HashMap<>());
            existingDataStreams.put(dataStream.getName(), dataStream);
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStreams));
            return this;
        }

        public Builder removeDataStream(String name) {
            Map<String, DataStream> existingDataStreams = Optional.ofNullable(
                (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE)
            ).map(dsmd -> new HashMap<>(dsmd.dataStreams())).orElse(new HashMap<>());
            existingDataStreams.remove(name);
            this.customs.put(DataStreamMetadata.TYPE, new DataStreamMetadata(existingDataStreams));
            return this;
        }

        public Builder queryGroups(final Map<String, QueryGroup> queryGroups) {
            this.customs.put(QueryGroupMetadata.TYPE, new QueryGroupMetadata(queryGroups));
            return this;
        }

        public Builder put(final QueryGroup queryGroup) {
            Objects.requireNonNull(queryGroup, "queryGroup should not be null");
            Map<String, QueryGroup> existing = new HashMap<>(getQueryGroups());
            existing.put(queryGroup.get_id(), queryGroup);
            return queryGroups(existing);
        }

        public Builder remove(final QueryGroup queryGroup) {
            Objects.requireNonNull(queryGroup, "queryGroup should not be null");
            Map<String, QueryGroup> existing = new HashMap<>(getQueryGroups());
            existing.remove(queryGroup.get_id());
            return queryGroups(existing);
        }

        private Map<String, QueryGroup> getQueryGroups() {
            return Optional.ofNullable(this.customs.get(QueryGroupMetadata.TYPE))
                .map(o -> (QueryGroupMetadata) o)
                .map(QueryGroupMetadata::queryGroups)
                .orElse(Collections.emptyMap());
        }

        private Map<String, View> getViews() {
            return Optional.ofNullable(customs.get(ViewMetadata.TYPE))
                .map(o -> (ViewMetadata) o)
                .map(vmd -> vmd.views())
                .orElse(new HashMap<>());
        }

        public View view(final String viewName) {
            return getViews().get(viewName);
        }

        public Builder views(final Map<String, View> views) {
            this.customs.put(ViewMetadata.TYPE, new ViewMetadata(views));
            return this;
        }

        public Builder put(final View view) {
            Objects.requireNonNull(view, "view cannot be null");
            final var replacementViews = new HashMap<>(getViews());
            replacementViews.put(view.getName(), view);
            return views(replacementViews);
        }

        public Builder removeView(final String viewName) {
            Objects.requireNonNull(viewName, "viewName cannot be null");
            final var replacementViews = new HashMap<>(getViews());
            replacementViews.remove(viewName);
            return views(replacementViews);
        }

        public Custom getCustom(String type) {
            return customs.get(type);
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(Map<String, Custom> customs) {
            StreamSupport.stream(Spliterators.spliterator(customs.entrySet(), 0), false)
                .forEach(cursor -> Objects.requireNonNull(cursor.getValue(), cursor.getKey()));
            this.customs.putAll(customs);
            return this;
        }

        public Builder indexGraveyard(final IndexGraveyard indexGraveyard) {
            putCustom(IndexGraveyard.TYPE, indexGraveyard);
            return this;
        }

        public IndexGraveyard indexGraveyard() {
            IndexGraveyard graveyard = (IndexGraveyard) getCustom(IndexGraveyard.TYPE);
            return graveyard;
        }

        public Builder decommissionAttributeMetadata(final DecommissionAttributeMetadata decommissionAttributeMetadata) {
            putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata);
            return this;
        }

        public DecommissionAttributeMetadata decommissionAttributeMetadata() {
            return (DecommissionAttributeMetadata) getCustom(DecommissionAttributeMetadata.TYPE);
        }

        public Builder updateSettings(Settings settings, String... indices) {
            if (indices == null || indices.length == 0) {
                indices = this.indices.keySet().toArray(new String[0]);
            }
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata).settings(Settings.builder().put(indexMetadata.getSettings()).put(settings)));
            }
            return this;
        }

        /**
         * Update the number of replicas for the specified indices.
         *
         * @param numberOfReplicas the number of replicas
         * @param indices          the indices to update the number of replicas for
         * @return the builder
         */
        public Builder updateNumberOfReplicas(final int numberOfReplicas, final String[] indices) {
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata).numberOfReplicas(numberOfReplicas));
            }
            return this;
        }

        /**
         * Update the number of search replicas for the specified indices.
         *
         * @param numberOfSearchReplicas the number of search replicas
         * @param indices          the indices to update the number of replicas for
         * @return the builder
         */
        public Builder updateNumberOfSearchReplicas(final int numberOfSearchReplicas, final String[] indices) {
            for (String index : indices) {
                IndexMetadata indexMetadata = this.indices.get(index);
                if (indexMetadata == null) {
                    throw new IndexNotFoundException(index);
                }
                put(IndexMetadata.builder(indexMetadata).numberOfSearchReplicas(numberOfSearchReplicas));
            }
            return this;
        }

        public Builder coordinationMetadata(CoordinationMetadata coordinationMetadata) {
            this.coordinationMetadata = coordinationMetadata;
            return this;
        }

        public Settings transientSettings() {
            return this.transientSettings;
        }

        public Builder transientSettings(Settings settings) {
            this.transientSettings = settings;
            return this;
        }

        public Settings persistentSettings() {
            return this.persistentSettings;
        }

        public Builder persistentSettings(Settings settings) {
            this.persistentSettings = settings;
            return this;
        }

        public DiffableStringMap hashesOfConsistentSettings() {
            return this.hashesOfConsistentSettings;
        }

        public Builder hashesOfConsistentSettings(DiffableStringMap hashesOfConsistentSettings) {
            this.hashesOfConsistentSettings = hashesOfConsistentSettings;
            return this;
        }

        public Builder hashesOfConsistentSettings(Map<String, String> hashesOfConsistentSettings) {
            this.hashesOfConsistentSettings = new DiffableStringMap(hashesOfConsistentSettings);
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder clusterUUIDCommitted(boolean clusterUUIDCommitted) {
            this.clusterUUIDCommitted = clusterUUIDCommitted;
            return this;
        }

        public Builder generateClusterUuidIfNeeded() {
            if (clusterUUID.equals(UNKNOWN_CLUSTER_UUID)) {
                clusterUUID = UUIDs.randomBase64UUID();
            }
            return this;
        }

        public Metadata build() {
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            DataStreamMetadata previousDataStreamMetadata = (previousMetadata != null)
                ? (DataStreamMetadata) this.previousMetadata.customs.get(DataStreamMetadata.TYPE)
                : null;

            buildSystemTemplatesLookup();

            boolean recomputeRequiredforIndicesLookups = (previousMetadata == null)
                || (indices.equals(previousMetadata.indices) == false)
                || (previousDataStreamMetadata != null && previousDataStreamMetadata.equals(dataStreamMetadata) == false)
                || (dataStreamMetadata != null && dataStreamMetadata.equals(previousDataStreamMetadata) == false);

            return (recomputeRequiredforIndicesLookups == false)
                ? buildMetadataWithPreviousIndicesLookups()
                : buildMetadataWithRecomputedIndicesLookups();
        }

        private void buildSystemTemplatesLookup() {
            if (previousMetadata != null
                && Objects.equals(
                    previousMetadata.customs.get(ComponentTemplateMetadata.TYPE),
                    this.customs.get(ComponentTemplateMetadata.TYPE)
                )) {
                systemTemplatesLookup = Collections.unmodifiableMap(previousMetadata.systemTemplatesLookup);
            } else {
                systemTemplatesLookup = new HashMap<>();
                Optional.ofNullable((ComponentTemplateMetadata) this.customs.get(ComponentTemplateMetadata.TYPE))
                    .map(ComponentTemplateMetadata::componentTemplates)
                    .orElseGet(Collections::emptyMap)
                    .forEach((k, v) -> {
                        if (MetadataIndexTemplateService.isSystemTemplate(v)) {
                            SystemTemplateMetadata templateMetadata = SystemTemplateMetadata.fromComponentTemplate(k);
                            systemTemplatesLookup.compute(templateMetadata.name(), (ik, iv) -> {
                                if (iv == null) {
                                    iv = new TreeMap<>();
                                }
                                iv.put(templateMetadata.version(), k);
                                return iv;
                            });
                        }
                    });
            }
        }

        protected Metadata buildMetadataWithPreviousIndicesLookups() {
            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                hashesOfConsistentSettings,
                indices,
                templates,
                customs,
                Arrays.copyOf(previousMetadata.allIndices, previousMetadata.allIndices.length),
                Arrays.copyOf(previousMetadata.visibleIndices, previousMetadata.visibleIndices.length),
                Arrays.copyOf(previousMetadata.allOpenIndices, previousMetadata.allOpenIndices.length),
                Arrays.copyOf(previousMetadata.visibleOpenIndices, previousMetadata.visibleOpenIndices.length),
                Arrays.copyOf(previousMetadata.allClosedIndices, previousMetadata.allClosedIndices.length),
                Arrays.copyOf(previousMetadata.visibleClosedIndices, previousMetadata.visibleClosedIndices.length),
                Collections.unmodifiableSortedMap(previousMetadata.indicesLookup),
                systemTemplatesLookup
            );
        }

        protected Metadata buildMetadataWithRecomputedIndicesLookups() {
            // TODO: We should move these datastructures to IndexNameExpressionResolver, this will give the following benefits:
            // 1) The datastructures will be rebuilt only when needed. Now during serializing we rebuild these datastructures
            // while these datastructures aren't even used.
            // 2) The aliasAndIndexLookup can be updated instead of rebuilding it all the time.

            final Set<String> allIndices = new HashSet<>(indices.size());
            final List<String> visibleIndices = new ArrayList<>();
            final List<String> allOpenIndices = new ArrayList<>();
            final List<String> visibleOpenIndices = new ArrayList<>();
            final List<String> allClosedIndices = new ArrayList<>();
            final List<String> visibleClosedIndices = new ArrayList<>();
            final Set<String> allAliases = new HashSet<>();
            for (final IndexMetadata indexMetadata : indices.values()) {
                final String name = indexMetadata.getIndex().getName();
                boolean added = allIndices.add(name);
                assert added : "double index named [" + name + "]";
                final boolean visible = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.getSettings()) == false;
                if (visible) {
                    visibleIndices.add(name);
                }
                if (indexMetadata.getState() == IndexMetadata.State.OPEN) {
                    allOpenIndices.add(name);
                    if (visible) {
                        visibleOpenIndices.add(name);
                    }
                } else if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    allClosedIndices.add(name);
                    if (visible) {
                        visibleClosedIndices.add(name);
                    }
                }
                indexMetadata.getAliases().keySet().iterator().forEachRemaining(allAliases::add);
            }

            final Set<String> allDataStreams = new HashSet<>();
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            if (dataStreamMetadata != null) {
                for (DataStream dataStream : dataStreamMetadata.dataStreams().values()) {
                    allDataStreams.add(dataStream.getName());
                }
            }

            final Set<String> aliasDuplicatesWithIndices = new HashSet<>(allAliases);
            aliasDuplicatesWithIndices.retainAll(allIndices);
            ArrayList<String> duplicates = new ArrayList<>();
            if (aliasDuplicatesWithIndices.isEmpty() == false) {
                // iterate again and constructs a helpful message
                for (final IndexMetadata cursor : indices.values()) {
                    for (String alias : aliasDuplicatesWithIndices) {
                        if (cursor.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.getIndex() + ") conflicts with index");
                        }
                    }
                }
            }

            final Set<String> aliasDuplicatesWithDataStreams = new HashSet<>(allAliases);
            aliasDuplicatesWithDataStreams.retainAll(allDataStreams);
            if (aliasDuplicatesWithDataStreams.isEmpty() == false) {
                // iterate again and constructs a helpful message
                for (final IndexMetadata cursor : indices.values()) {
                    for (String alias : aliasDuplicatesWithDataStreams) {
                        if (cursor.getAliases().containsKey(alias)) {
                            duplicates.add(alias + " (alias of " + cursor.getIndex() + ") conflicts with data stream");
                        }
                    }
                }
            }

            final Set<String> dataStreamDuplicatesWithIndices = new HashSet<>(allDataStreams);
            dataStreamDuplicatesWithIndices.retainAll(allIndices);
            if (dataStreamDuplicatesWithIndices.isEmpty() == false) {
                for (String dataStream : dataStreamDuplicatesWithIndices) {
                    duplicates.add("data stream [" + dataStream + "] conflicts with index");
                }
            }

            if (duplicates.size() > 0) {
                throw new IllegalStateException(
                    "index, alias, and data stream names need to be unique, but the following duplicates "
                        + "were found ["
                        + Strings.collectionToCommaDelimitedString(duplicates)
                        + "]"
                );
            }

            SortedMap<String, IndexAbstraction> indicesLookup = Collections.unmodifiableSortedMap(buildIndicesLookup());

            validateDataStreams(indicesLookup, (DataStreamMetadata) customs.get(DataStreamMetadata.TYPE));

            // build all concrete indices arrays:
            // TODO: I think we can remove these arrays. it isn't worth the effort, for operations on all indices.
            // When doing an operation across all indices, most of the time is spent on actually going to all shards and
            // do the required operations, the bottleneck isn't resolving expressions into concrete indices.
            String[] allIndicesArray = allIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleIndicesArray = visibleIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allOpenIndicesArray = allOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleOpenIndicesArray = visibleOpenIndices.toArray(Strings.EMPTY_ARRAY);
            String[] allClosedIndicesArray = allClosedIndices.toArray(Strings.EMPTY_ARRAY);
            String[] visibleClosedIndicesArray = visibleClosedIndices.toArray(Strings.EMPTY_ARRAY);

            return new Metadata(
                clusterUUID,
                clusterUUIDCommitted,
                version,
                coordinationMetadata,
                transientSettings,
                persistentSettings,
                hashesOfConsistentSettings,
                indices,
                templates,
                customs,
                allIndicesArray,
                visibleIndicesArray,
                allOpenIndicesArray,
                visibleOpenIndicesArray,
                allClosedIndicesArray,
                visibleClosedIndicesArray,
                indicesLookup,
                systemTemplatesLookup
            );
        }

        private SortedMap<String, IndexAbstraction> buildIndicesLookup() {
            SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
            Map<String, DataStream> indexToDataStreamLookup = new HashMap<>();
            DataStreamMetadata dataStreamMetadata = (DataStreamMetadata) this.customs.get(DataStreamMetadata.TYPE);
            // If there are no indices, then skip data streams. This happens only when metadata is read from disk
            if (dataStreamMetadata != null && indices.size() > 0) {
                for (DataStream dataStream : dataStreamMetadata.dataStreams().values()) {
                    List<IndexMetadata> backingIndices = dataStream.getIndices()
                        .stream()
                        .map(index -> indices.get(index.getName()))
                        .collect(Collectors.toList());
                    assert backingIndices.isEmpty() == false;
                    assert backingIndices.contains(null) == false;

                    IndexAbstraction existing = indicesLookup.put(
                        dataStream.getName(),
                        new IndexAbstraction.DataStream(dataStream, backingIndices)
                    );
                    assert existing == null : "duplicate data stream for " + dataStream.getName();

                    for (Index i : dataStream.getIndices()) {
                        indexToDataStreamLookup.put(i.getName(), dataStream);
                    }
                }
            }

            for (final IndexMetadata indexMetadata : indices.values()) {
                IndexAbstraction.Index index;
                DataStream parent = indexToDataStreamLookup.get(indexMetadata.getIndex().getName());
                if (parent != null) {
                    assert parent.getIndices().contains(indexMetadata.getIndex());
                    index = new IndexAbstraction.Index(indexMetadata, (IndexAbstraction.DataStream) indicesLookup.get(parent.getName()));
                } else {
                    index = new IndexAbstraction.Index(indexMetadata);
                }

                IndexAbstraction existing = indicesLookup.put(indexMetadata.getIndex().getName(), index);
                assert existing == null : "duplicate for " + indexMetadata.getIndex();

                for (final AliasMetadata aliasMetadata : indexMetadata.getAliases().values()) {
                    indicesLookup.compute(aliasMetadata.getAlias(), (aliasName, alias) -> {
                        if (alias == null) {
                            return new IndexAbstraction.Alias(aliasMetadata, indexMetadata);
                        } else {
                            assert alias.getType() == IndexAbstraction.Type.ALIAS : alias.getClass().getName();
                            ((IndexAbstraction.Alias) alias).addIndex(indexMetadata);
                            return alias;
                        }
                    });
                }
            }

            indicesLookup.values()
                .stream()
                .filter(aliasOrIndex -> aliasOrIndex.getType() == IndexAbstraction.Type.ALIAS)
                .forEach(alias -> ((IndexAbstraction.Alias) alias).computeAndValidateAliasProperties());
            return indicesLookup;
        }

        /**
         * Validates there isn't any index with a name that would clash with the future backing indices of the existing data streams.
         * <p>
         * E.g., if data stream `foo` has backing indices [`.ds-foo-000001`, `.ds-foo-000002`] and the indices lookup contains indices
         * `.ds-foo-000001`, `.ds-foo-000002` and `.ds-foo-000006` this will throw an IllegalStateException (as attempting to rollover the
         * `foo` data stream from generation 5 to 6 will not be possible)
         *
         * @param indicesLookup the indices in the system (this includes the data stream backing indices)
         * @param dsMetadata    the data streams in the system
         */
        static void validateDataStreams(SortedMap<String, IndexAbstraction> indicesLookup, @Nullable DataStreamMetadata dsMetadata) {
            if (dsMetadata != null) {
                for (DataStream ds : dsMetadata.dataStreams().values()) {
                    String prefix = DataStream.BACKING_INDEX_PREFIX + ds.getName() + "-";
                    Set<String> conflicts = indicesLookup.subMap(prefix, DataStream.BACKING_INDEX_PREFIX + ds.getName() + ".")
                        .keySet()
                        .stream()
                        .filter(s -> NUMBER_PATTERN.matcher(s.substring(prefix.length())).matches())
                        .filter(s -> IndexMetadata.parseIndexNameCounter(s) > ds.getGeneration())
                        .collect(Collectors.toSet());

                    if (conflicts.size() > 0) {
                        throw new IllegalStateException(
                            "data stream ["
                                + ds.getName()
                                + "] could create backing indices that conflict with "
                                + conflicts.size()
                                + " existing index(s) or alias(s)"
                                + " including '"
                                + conflicts.iterator().next()
                                + "'"
                        );
                    }
                }
            }
        }

        public static void toXContent(Metadata metadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            XContentContext context = XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));

            if (context == XContentContext.API) {
                builder.startObject("metadata");
            } else {
                builder.startObject("meta-data");
                builder.field("version", metadata.version());
            }

            builder.field("cluster_uuid", metadata.clusterUUID);
            builder.field("cluster_uuid_committed", metadata.clusterUUIDCommitted);

            builder.startObject("cluster_coordination");
            metadata.coordinationMetadata().toXContent(builder, params);
            builder.endObject();

            if (context != XContentContext.API && !metadata.persistentSettings().isEmpty()) {
                builder.startObject("settings");
                metadata.persistentSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
                builder.endObject();
            }

            builder.startObject("templates");
            metadata.templatesMetadata().toXContent(builder, params);
            builder.endObject();

            if (context == XContentContext.API) {
                builder.startObject("indices");
                for (IndexMetadata indexMetadata : metadata) {
                    IndexMetadata.Builder.toXContent(indexMetadata, builder, params);
                }
                builder.endObject();
            }

            for (final Map.Entry<String, Custom> cursor : metadata.customs().entrySet()) {
                if (cursor.getValue().context().contains(context)) {
                    builder.startObject(cursor.getKey());
                    cursor.getValue().toXContent(builder, params);
                    builder.endObject();
                }
            }
            builder.endObject();
        }

        public static Metadata fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            // we might get here after the meta-data element, or on a fresh parser
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (!"meta-data".equals(currentFieldName)) {
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    // move to the field name (meta-data)
                    token = parser.nextToken();
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new IllegalArgumentException("Expected a field name but got " + token);
                    }
                    // move to the next object
                    token = parser.nextToken();
                }
                currentFieldName = parser.currentName();
            }

            if (!"meta-data".equals(parser.currentName())) {
                throw new IllegalArgumentException("Expected [meta-data] as a field name but got " + currentFieldName);
            }
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected a START_OBJECT but got " + token);
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("cluster_coordination".equals(currentFieldName)) {
                        builder.coordinationMetadata(CoordinationMetadata.fromXContent(parser));
                    } else if ("settings".equals(currentFieldName)) {
                        builder.persistentSettings(Settings.fromXContent(parser));
                    } else if ("indices".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.put(IndexMetadata.Builder.fromXContent(parser), false);
                        }
                    } else if ("hashes_of_consistent_settings".equals(currentFieldName)) {
                        builder.hashesOfConsistentSettings(parser.mapStrings());
                    } else if ("templates".equals(currentFieldName)) {
                        builder.templates(TemplatesMetadata.fromXContent(parser));
                    } else {
                        try {
                            Custom custom = Custom.fromXContent(parser, currentFieldName);
                            builder.putCustom(custom.getWriteableName(), custom);
                        } catch (NamedObjectNotFoundException ex) {
                            logger.warn("Skipping unknown custom object with type {}", currentFieldName);
                            parser.skipChildren();
                        }
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version = parser.longValue();
                    } else if ("cluster_uuid".equals(currentFieldName) || "uuid".equals(currentFieldName)) {
                        builder.clusterUUID = parser.text();
                    } else if ("cluster_uuid_committed".equals(currentFieldName)) {
                        builder.clusterUUIDCommitted = parser.booleanValue();
                    } else {
                        throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            return builder.build();
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new MapParams(params);
    }

    /**
     * State format for {@link Metadata} to write to and load from disk
     */
    public static final MetadataStateFormat<Metadata> FORMAT = new MetadataStateFormat<Metadata>(GLOBAL_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, Metadata state) throws IOException {
            Builder.toXContent(state, builder, FORMAT_PARAMS);
        }

        @Override
        public Metadata fromXContent(XContentParser parser) throws IOException {
            return Builder.fromXContent(parser);
        }
    };

}
