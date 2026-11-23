/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Extension point that lets a plugin change a search request on the coordinating node, knowing the
 * concrete indices the request targets and their mappings.
 * <p>
 * Processors are registered through {@link SearchPlugin#getIndexAwareSearchRequestProcessors()} and run
 * only while {@code search.index_aware_search_request_processors.enabled} is {@code true}. Each one runs
 * once per search request, before the coordinator query rewrite: by default after the request's search
 * pipeline request processors, or before them if the processor asks for that with
 * {@link #getExecutionStage()}. Whatever a processor produces is therefore rewritten and sent to the
 * shards like a request the caller had written that way.
 * <p>
 * An index-aware processor differs from a search pipeline request processor in two ways:
 * <ul>
 *   <li>it receives an {@link IndexAwareContext}: the concrete local indices the request targets and each
 *       one's metadata and mapping, so that what it does can depend on how each index maps a field;</li>
 *   <li>it is not part of a search pipeline, so it runs whichever pipeline the request uses, including
 *       none ({@code search_pipeline=_none}).</li>
 * </ul>
 * A processor may change any part of the source (the query, {@code size}, aggregations), not only the
 * query.
 * <p>
 * <b>Searches a processor does not run for.</b>
 * <ul>
 *   <li>A search marked as a sub-search with {@link SubSearchMarker}, so that a search a processor issues
 *       itself is not processed again.</li>
 *   <li>A cross-cluster search: one that targets a remote index, and one a remote coordinating node
 *       forwarded to this cluster. See {@link IndexAwareSearchRequestScope#hasRemoteIndices}.</li>
 *   <li>An internal search (the search behind a by-query or reindex job, or a search of a system index),
 *       unless the processor opts in with {@link #appliesToInternalSearches()}. See
 *       {@link IndexAwareSearchRequestScope#isInternalByDefault}.</li>
 *   <li>A search the coordinating node issues as part of a search that has already been processed, such
 *       as the search per group of a collapse with inner hits.</li>
 *   <li>A request without a source, and the creation of a point in time, which runs no query.</li>
 * </ul>
 * <p>
 * <b>Indices that map a field differently.</b> One source is sent to every index the request targets, but
 * a request over an alias, a wildcard or several indices can target indices that map the same field
 * differently, or that do not all have it. The context reports each index separately for that reason. A
 * processor must resolve a source that is correct for every target index: for example by leaving the
 * request alone unless all indices agree, or by producing one clause per group of indices, each limited
 * to its indices with a filter on {@code _index}.
 * <p>
 * <b>Failures.</b> A processor that throws, or fails its listener, fails the search with that exception,
 * and neither the remaining processors nor the search run. A processor can change what a query means, so
 * running the search without it would answer a different question than the one the index is configured
 * to answer. A processor whose absence only makes results less good, not wrong, can opt out with
 * {@link #ignoreFailure()}.
 * <p>
 * <b>Security.</b> Processors run after the node's action filters, which is where a security plugin
 * authorizes a request. A request that was rejected never reaches a processor, and the context holds only
 * the indices the request targets once filters have had their say. A filter may also have added its own
 * restriction to the source (for example a document-level security filter wrapped around the caller's
 * query): that restriction is part of the source a processor receives, so a processor must replace only
 * the clauses it recognizes and keep the rest of the query tree as it found it. Restrictions data nodes
 * apply themselves, such as alias filters, apply to whatever source a processor resolves. Requests a
 * processor issues run in the caller's thread context, with the caller's identity, unless the processor
 * replaces that context. {@link IndexAwareContext#getFieldFilter} tells a processor which fields the
 * caller may see.
 * <p>
 * <b>No blocking.</b> {@link #process} reports its result through a listener, so a processor that has to
 * wait for something (a sub-search, a call to another service) does so without holding a thread. It must
 * not block the calling thread: that may be a transport thread, or a thread of the search pool that the
 * processor's own sub-search needs.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexAwareSearchRequestProcessor {

    /**
     * Changes the search request's source, or leaves it as it is, and passes the result to
     * {@code listener}.
     * <p>
     * <b>The source.</b> {@code source} is a shallow copy of the request's source, made for the processors
     * of this request. Setting one of its parts, as in {@code source.query(newQuery)} or
     * {@code source.size(n)}, does not affect the {@code SearchRequest} the caller passed in, so a caller
     * that reuses or retries its request sends its original source again. The builders the copy refers to
     * (the query and its clauses, aggregations, sorts and so on) are the caller's own instances, not
     * copies. Do not modify one of them in place; build a replacement and set it. A builder a processor
     * leaves alone stays the same instance, which matters to code that keeps a reference to a builder of
     * the request.
     * <p>
     * <b>The listener.</b> A processor with nothing to wait for calls {@code listener.onResponse} before
     * returning. One that has to wait calls it from the callback of whatever it waited for. The listener
     * must be completed exactly once on every path, including every failure path of anything the
     * processor waits for: a processor that never completes it leaves the search waiting until the search
     * task is cancelled. A second completion is ignored.
     * <p>
     * <b>Thread context.</b> The search continues in the thread context {@code process} was called in,
     * whichever thread and context the listener is completed from.
     *
     * @param source   the search request's source, as left by whatever ran before this processor: the
     *                 request's search pipeline, for an {@link ExecutionStage#AFTER_PIPELINE} processor,
     *                 and any processor of the same stage that was registered earlier
     * @param context  the indices the request targets, their metadata and mappings, and request-level
     *                 information
     * @param listener receives the source to continue with: {@code source} itself, changed or not, or a
     *                 different instance; never {@code null}
     */
    void process(SearchSourceBuilder source, IndexAwareContext context, ActionListener<SearchSourceBuilder> listener);

    /**
     * A cheap check, on the source alone, of whether this processor could have anything to do for a
     * request: for example whether the source has a query at all, or contains the kind of clause this
     * processor changes. It is called for every search before anything else is done on behalf of this
     * processor. If no registered processor returns {@code true}, the request is left alone entirely: its
     * indices are not resolved, no mapping is read, and its source is not copied.
     * <p>
     * A processor that can rule out most requests this way should override this method; it is what keeps
     * the cost of the extension point off requests the processor does not apply to.
     *
     * @param source the request's own source, not a copy; it must not be modified
     * @return {@code false} if this processor has nothing to do for the request; {@code true} by default
     */
    default boolean mayApplyTo(SearchSourceBuilder source) {
        return true;
    }

    /**
     * Whether the search continues without this processor when the processor fails.
     * <p>
     * If {@code true}, a failure is logged and the remaining processors run on the source this processor
     * was given, so the processor must not have changed that source before failing. Cancellation, circuit
     * breaker and thread pool rejection failures fail the search regardless: they mean the node cannot do
     * the work, not that the processor is broken.
     *
     * @return {@code false} by default: a failure of this processor fails the search. Return {@code true}
     *         only if results without this processor are still correct, just less good.
     */
    default boolean ignoreFailure() {
        return false;
    }

    /**
     * Whether this processor also runs for an internal search, as classified by
     * {@link IndexAwareSearchRequestScope#isInternalByDefault}: the search behind a by-query or reindex
     * job, or a search of a system index. The answer covers both kinds; a processor cannot opt into one
     * without the other.
     *
     * @return {@code false} by default. Return {@code true} only if what this processor does is safe for
     *         a job that deletes, updates or copies exactly the documents its search matches.
     */
    default boolean appliesToInternalSearches() {
        return false;
    }

    /**
     * A name for this processor, used in log messages about it.
     *
     * @return the simple name of the implementing class by default. That is empty for a lambda or an
     *         anonymous class, so a processor that should be identifiable in logs overrides this.
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    /**
     * When this processor runs relative to the request's search pipeline.
     * <p>
     * A {@link ExecutionStage#BEFORE_PIPELINE} processor is given a context resolved from the request as
     * the caller sent it. If a search pipeline request processor then changes which indices the request
     * targets, that context no longer describes the indices the search runs against.
     *
     * @return {@link ExecutionStage#AFTER_PIPELINE} by default
     */
    default ExecutionStage getExecutionStage() {
        return ExecutionStage.AFTER_PIPELINE;
    }

    /**
     * When a processor runs relative to the request's search pipeline. The pipeline here is everything
     * search pipelines do to a request: the pipeline the request names or its indices default to, and the
     * system-generated processors that run before and after it.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    enum ExecutionStage {
        /** Before the search pipeline, whose request processors then see what this processor produced. */
        BEFORE_PIPELINE,
        /**
         * After the search pipeline and before the coordinator query rewrite. The processor sees what the
         * pipeline's request processors produced.
         */
        AFTER_PIPELINE
    }
}
