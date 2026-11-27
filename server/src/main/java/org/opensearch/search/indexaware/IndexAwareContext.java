/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugins.MapperPlugin;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * What an {@link IndexAwareSearchRequestProcessor} is told about the search request it is processing: the
 * concrete indices the request targets, with their metadata and mappings, plus the request-level facts a
 * processor needs to behave the way the rest of the search does.
 * <p>
 * <b>Local indices only.</b> A search that targets a remote cluster never reaches a processor; see
 * {@link IndexAwareSearchRequestScope#hasRemoteIndices}.
 * <p>
 * <b>Indices can disagree.</b> A request over an alias, a wildcard or a list of indices may target indices
 * that map the same field differently, or that do not all have it. {@link #getTargetIndices()} and
 * {@link #getFieldMappings(String)} are per index for that reason: a processor decides per index, and
 * must resolve a source that is correct for every index in the request, not only for the first one it
 * looks at.
 * <p>
 * An instance is built for a request and passed to each processor in turn. It is immutable and safe to use
 * from whichever thread a processor continues on.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class IndexAwareContext {

    private final Map<String, IndexMetadata> targetIndices;
    private final Function<IndexMetadata, Map<String, Object>> fieldMappingsLookup;
    private final Function<String, Predicate<String>> fieldFilter;
    private final Version minNodeVersion;
    private final TaskId searchTaskId;
    private final boolean allowPartialSearchResults;
    private final String parentAction;
    private final boolean byQuerySearch;

    private IndexAwareContext(Builder builder, Function<IndexMetadata, Map<String, Object>> fieldMappingsLookup) {
        // A copy: the context is immutable, whatever its builder's caller does with the map later.
        this.targetIndices = Map.copyOf(builder.targetIndices);
        this.fieldMappingsLookup = fieldMappingsLookup;
        this.fieldFilter = builder.fieldFilter;
        this.minNodeVersion = builder.minNodeVersion;
        this.searchTaskId = builder.searchTaskId;
        this.allowPartialSearchResults = builder.allowPartialSearchResults;
        this.parentAction = builder.parentAction;
        this.byQuerySearch = builder.byQuerySearch;
    }

    /**
     * Starts building the context for one request. Core builds it when it runs the processors, and sets
     * every value; a plugin only needs this to test a processor, and sets only the values that test is
     * about.
     *
     * @param targetIndices the concrete local indices the request targets, keyed by index name
     * @return a builder whose other values have the defaults documented on its setters
     */
    public static Builder builder(Map<String, IndexMetadata> targetIndices) {
        return new Builder(targetIndices);
    }

    /**
     * The concrete local indices the request targets, keyed by index name, each with its
     * {@link IndexMetadata} (mapping, settings, aliases) from the cluster state the request was resolved
     * against. Aliases, wildcards, date math and data streams are already resolved, with the request's own
     * indices options, so an index the request is allowed to skip (for example a missing index under
     * {@code ignore_unavailable}) is simply absent.
     *
     * @return an unmodifiable map from index name to index metadata
     */
    public Map<String, IndexMetadata> getTargetIndices() {
        return targetIndices;
    }

    /**
     * The field mappings of one target index: the {@code properties} section of its mapping, as a map from
     * field name to that field's mapping. For example, the mapping of a {@code title} field is
     * {@code getFieldMappings(indexName).get("title")}, and its sub-fields and object properties are nested
     * inside it as they are in the mapping. The rest of the mapping ({@code dynamic}, {@code _meta},
     * {@code dynamic_templates} and so on) is not included; {@link #getTargetIndices()} has the complete
     * mapping in each index's {@link IndexMetadata}.
     * <p>
     * The map comes from the context's field-mappings lookup. During a search, core uses the node-level
     * {@link IndexMappingSourceCache}, so the map is usually parsed once per mapping version rather than once
     * per request, and it is shared with every other request on the node: it is unmodifiable at every
     * level. A context built for a test without a lookup parses through a cache of its own, with the same
     * guarantees.
     * <p>
     * These are all of the index's fields. They are not reduced by {@link #getFieldFilter(String)}.
     *
     * @param indexName the name of one of {@link #getTargetIndices()}
     * @return the field mappings, or an empty map if {@code indexName} is not a target index or its mapping
     *         has no fields
     */
    public Map<String, Object> getFieldMappings(String indexName) {
        final IndexMetadata metadata = targetIndices.get(indexName);
        if (metadata == null) {
            return Collections.emptyMap();
        }
        final Map<String, Object> fieldMappings = fieldMappingsLookup.apply(metadata);
        return fieldMappings == null ? Collections.emptyMap() : fieldMappings;
    }

    /**
     * The fields of one target index that the caller is allowed to see: the predicate plugins contribute
     * through {@link MapperPlugin#getFieldFilter()}, which the get-mapping, get-field-mapping and field
     * capabilities APIs also apply. A security plugin uses it to hide fields from a user (field-level
     * security).
     * <p>
     * A processor acts with the node's knowledge of the mapping, not the caller's. Before it acts on a
     * field the caller named, it should check that field here, so that a change to the query does not
     * reveal, through different results or errors, a field the caller cannot see.
     * <p>
     * A plugin's predicate may read the caller's identity from the thread context, so apply it on the
     * thread {@link IndexAwareSearchRequestProcessor#process} was called on, before any asynchronous work.
     *
     * @param indexName the name of one of {@link #getTargetIndices()}
     * @return a predicate that accepts the names of the fields the caller may see in that index
     */
    public Predicate<String> getFieldFilter(String indexName) {
        return fieldFilter.apply(indexName);
    }

    /**
     * Whether the request accepts partial results ({@code allow_partial_search_results}), with the
     * cluster default applied if the request does not say.
     * <p>
     * A processor that cannot do its work for some of the target indices, for example because a resource
     * one index's field depends on is unavailable, should treat that the way the search treats a failing
     * shard. If this returns {@code false}, fail the request by failing the listener. If it returns
     * {@code true}, the processor may instead leave that index's part of the query unchanged and let the
     * rest of the request proceed.
     *
     * @return {@code true} if the request accepts partial results
     */
    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    /**
     * The oldest node version in the cluster. The source a processor resolves is sent to every data node
     * that holds a target shard, so during a rolling upgrade a processor must not produce a query,
     * aggregation or other named writeable that nodes on this version cannot read.
     *
     * @return the minimum node version in the cluster state the request was resolved against
     */
    public Version getMinNodeVersion() {
        return minNodeVersion;
    }

    /**
     * The id of the search task the processor is running for. A processor that issues requests of its own
     * (a sub-search, a get, a call to another service) should set it as their parent task with
     * {@code request.setParentTask(...)}: cancelling the search then cancels them too, and their resource
     * usage is attributed to the search that caused it.
     *
     * @return the search's task id, or {@link TaskId#EMPTY_TASK_ID} if the search has no task
     */
    public TaskId getSearchTaskId() {
        return searchTaskId;
    }

    /**
     * The action of the task that issued this search, if any: the same value a system-generated search
     * processor factory receives as {@code ProcessorGenerationContext#parentAction()}. For example, the
     * search behind a {@code _delete_by_query}, {@code _update_by_query} or {@code _reindex} job has the
     * job's action ({@link org.opensearch.index.reindex.DeleteByQueryAction#NAME},
     * {@link org.opensearch.index.reindex.UpdateByQueryAction#NAME} or
     * {@link org.opensearch.index.reindex.ReindexAction#NAME}), also for each slice of a sliced job.
     * <p>
     * Processors run for those searches like for any other, as search pipelines do, so the job acts on the
     * documents a {@code _search} with the same query returns. A processor whose rewrite matches by
     * similarity rather than by an exact condition may prefer to leave such a search alone, so that the job
     * acts only on the documents the caller's own query matches. Similarly, a processor that should not act
     * on system indices can check {@link IndexMetadata#isSystem()} in {@link #getTargetIndices()}.
     *
     * @return the parent task's action, or {@code null} if the search has no parent task, its parent runs
     *         on another node, or its parent is no longer registered
     */
    @Nullable
    public String getParentAction() {
        return parentAction;
    }

    /**
     * Whether this is the search behind a {@code _delete_by_query}, {@code _update_by_query} or
     * {@code _reindex} job, or behind one slice of a sliced job: its parent task is the job's
     * {@link org.opensearch.index.reindex.BulkByScrollTask}. The documents this search matches are the
     * documents the job deletes, updates or copies; see the by-query section of
     * {@link IndexAwareSearchRequestProcessor}.
     *
     * @return {@code true} if the search belongs to a by-query or reindex job
     */
    public boolean isByQuerySearch() {
        return byQuerySearch;
    }

    /**
     * Builds an {@link IndexAwareContext}. Obtain one with {@link IndexAwareContext#builder}.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class Builder {
        private final Map<String, IndexMetadata> targetIndices;
        private Function<IndexMetadata, Map<String, Object>> fieldMappingsLookup;
        private Function<String, Predicate<String>> fieldFilter = MapperPlugin.NOOP_FIELD_FILTER;
        private Version minNodeVersion = Version.CURRENT;
        private TaskId searchTaskId = TaskId.EMPTY_TASK_ID;
        private boolean allowPartialSearchResults = true;
        private String parentAction;
        private boolean byQuerySearch;

        private Builder(Map<String, IndexMetadata> targetIndices) {
            this.targetIndices = Objects.requireNonNull(targetIndices, "targetIndices");
        }

        /**
         * @param fieldMappingsLookup returns the field mappings of an index, as
         *                            {@link IndexAwareContext#getFieldMappings(String)} documents them. Core
         *                            passes the lookup of the node's {@link IndexMappingSourceCache}, which
         *                            parses each mapping version once for every request. Defaults to the
         *                            lookup of a cache of this context's own, so a test gets the same parsing
         *                            without sharing it with other contexts.
         * @return this builder
         */
        public Builder fieldMappingsLookup(Function<IndexMetadata, Map<String, Object>> fieldMappingsLookup) {
            this.fieldMappingsLookup = Objects.requireNonNull(fieldMappingsLookup, "fieldMappingsLookup");
            return this;
        }

        /**
         * @param fieldFilter the node's field filter, as {@code IndicesService#getFieldFilter()} returns it;
         *                    defaults to {@link MapperPlugin#NOOP_FIELD_FILTER}, which hides no field
         * @return this builder
         */
        public Builder fieldFilter(Function<String, Predicate<String>> fieldFilter) {
            this.fieldFilter = Objects.requireNonNull(fieldFilter, "fieldFilter");
            return this;
        }

        /**
         * @param minNodeVersion the oldest node version in the cluster; defaults to {@link Version#CURRENT}
         * @return this builder
         */
        public Builder minNodeVersion(Version minNodeVersion) {
            this.minNodeVersion = Objects.requireNonNull(minNodeVersion, "minNodeVersion");
            return this;
        }

        /**
         * @param searchTaskId the search's task id; defaults to {@link TaskId#EMPTY_TASK_ID}
         * @return this builder
         */
        public Builder searchTaskId(TaskId searchTaskId) {
            this.searchTaskId = Objects.requireNonNull(searchTaskId, "searchTaskId");
            return this;
        }

        /**
         * @param allowPartialSearchResults whether the request accepts partial results, with the cluster
         *                                  default already applied; defaults to {@code true}, the default of
         *                                  {@code search.default_allow_partial_results}
         * @return this builder
         */
        public Builder allowPartialSearchResults(boolean allowPartialSearchResults) {
            this.allowPartialSearchResults = allowPartialSearchResults;
            return this;
        }

        /**
         * @param parentAction the action of the task that issued the search; defaults to {@code null}, a
         *                     search without a parent task
         * @return this builder
         */
        public Builder parentAction(@Nullable String parentAction) {
            this.parentAction = parentAction;
            return this;
        }

        /**
         * @param byQuerySearch whether the search belongs to a by-query or reindex job; defaults to
         *                      {@code false}
         * @return this builder
         */
        public Builder byQuerySearch(boolean byQuerySearch) {
            this.byQuerySearch = byQuerySearch;
            return this;
        }

        /** @return the context; each call returns a new context, with a cache of its own if no lookup was set */
        public IndexAwareContext build() {
            return new IndexAwareContext(
                this,
                fieldMappingsLookup != null ? fieldMappingsLookup : new IndexMappingSourceCache()::getFieldMappings
            );
        }
    }
}
