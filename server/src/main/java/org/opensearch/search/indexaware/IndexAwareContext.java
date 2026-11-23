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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugins.MapperPlugin;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * What an {@code IndexAwareSearchRequestProcessor} is told about the search request it is processing: the
 * concrete indices the request targets, with their metadata and mappings, plus the request-level facts a
 * processor needs to behave the way the rest of the search does.
 * <p>
 * <b>Local indices only.</b> A search that targets a remote cluster never reaches a processor; see
 * {@link IndexAwareSearchRequestScope#hasRemoteIndices}.
 * <p>
 * <b>Indices can disagree.</b> A request over an alias, a wildcard or a list of indices may target indices
 * that map the same field differently, or that do not all have it. {@link #getTargetIndices()} and
 * {@link #getMappingSourceAsMap(String)} are per index for that reason: a processor decides per index, and
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
    private final IndexMappingSourceCache mappingSourceCache;
    private final Function<String, Predicate<String>> fieldFilter;
    private final Version minNodeVersion;
    private final TaskId searchTaskId;
    private final boolean allowPartialSearchResults;

    /**
     * Creates the context for one request. Core builds it when it runs the processors; a plugin only
     * needs this constructor to test a processor.
     *
     * @param targetIndices             the concrete local indices the request targets, keyed by index name
     * @param mappingSourceCache        the node's cache of parsed mappings
     * @param fieldFilter               the node's field filter, as {@code IndicesService#getFieldFilter()}
     *                                  returns it
     * @param minNodeVersion            the oldest node version in the cluster
     * @param searchTaskId              the search's task id, or {@link TaskId#EMPTY_TASK_ID}
     * @param allowPartialSearchResults whether the request accepts partial results, with the cluster
     *                                  default already applied
     */
    public IndexAwareContext(
        Map<String, IndexMetadata> targetIndices,
        IndexMappingSourceCache mappingSourceCache,
        Function<String, Predicate<String>> fieldFilter,
        Version minNodeVersion,
        TaskId searchTaskId,
        boolean allowPartialSearchResults
    ) {
        this.targetIndices = Collections.unmodifiableMap(targetIndices);
        this.mappingSourceCache = mappingSourceCache;
        this.fieldFilter = fieldFilter;
        this.minNodeVersion = minNodeVersion;
        this.searchTaskId = searchTaskId;
        this.allowPartialSearchResults = allowPartialSearchResults;
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
     * The mapping of one target index, parsed into a {@code Map}. The map comes from the node-level
     * {@link IndexMappingSourceCache}, so it is parsed once per mapping version rather than once per
     * request, and it is shared with every other request on the node: it is unmodifiable at every level.
     * <p>
     * This is the index's complete mapping. It is not reduced by {@link #getFieldFilter(String)}.
     *
     * @param indexName the name of one of {@link #getTargetIndices()}
     * @return the parsed mapping, or an empty map if {@code indexName} is not a target index or has no
     *         mapping
     */
    public Map<String, Object> getMappingSourceAsMap(String indexName) {
        final IndexMetadata metadata = targetIndices.get(indexName);
        if (metadata == null) {
            return Collections.emptyMap();
        }
        return mappingSourceCache.getMappingSourceAsMap(metadata);
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
     * thread {@code IndexAwareSearchRequestProcessor#process} was called on, before any asynchronous work.
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
}
