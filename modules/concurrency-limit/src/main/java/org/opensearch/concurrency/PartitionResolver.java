/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Maps an incoming request to a partition name for use with the partitioned concurrency limiter.
 * <p>
 * Returning {@code null} routes the request to the built-in {@code unknownPartition}, which
 * holds any unallocated share of the total limit.
 */
@FunctionalInterface
public interface PartitionResolver {

    /**
     * Returns the partition name for this request, or {@code null} to route to
     * {@code unknownPartition}.
     *
     * @param ctx the request context
     */
    String resolve(SearchRequestContext ctx);

    /**
     * Factory — creates the appropriate resolver from the alias configuration.
     *
     * @param resolverType  value of {@code partition.resolver} setting (e.g. {@code "byHeader"})
     * @param resolverConfig sub-settings under {@code partition.resolver.<type>.*}
     * @param threadContext  request-scoped thread context (used by {@code byHeader} resolver)
     */
    static PartitionResolver build(String resolverType, Settings resolverConfig, ThreadContext threadContext) {
        if ("byHeader".equals(resolverType)) {
            return new ByHeaderPartitionResolver(resolverConfig, threadContext);
        } else if ("fixed".equals(resolverType)) {
            return new FixedPartitionResolver(resolverConfig);
        } else if ("bySearchType".equals(resolverType)) {
            return new BySearchTypePartitionResolver(resolverConfig);
        }
        return ctx -> null;  // unknown type → unknownPartition
    }

    // -------------------------------------------------------------------------
    // Built-in implementations
    // -------------------------------------------------------------------------

    /**
     * Reads a named HTTP header from the thread context. The header value must match
     * a configured partition name exactly (case-sensitive). Requests without the
     * header, or with an unrecognised value, fall through to {@code unknownPartition}.
     * <p>
     * Configuration: {@code partition.resolver.byHeader.name = X-Request-Tier}
     * <p>
     * <b>Important:</b> only headers registered at node startup via
     * {@link ActionConcurrencyLimitPlugin#getRestHeaders()} are propagated into the
     * transport {@link ThreadContext}. By default this resolver reads
     * {@link ActionConcurrencyLimitPlugin#TIER_HEADER}. Configuring a different header
     * name will silently resolve to {@code null} (→ {@code unknownPartition}) unless that
     * header is also registered in {@code getRestHeaders()}.
     */
    final class ByHeaderPartitionResolver implements PartitionResolver {
        private final String headerName;
        private final ThreadContext threadContext;

        ByHeaderPartitionResolver(Settings config, ThreadContext threadContext) {
            this.headerName = config.get("name", ActionConcurrencyLimitPlugin.TIER_HEADER);
            this.threadContext = threadContext;
        }

        @Override
        public String resolve(SearchRequestContext ctx) {
            // ThreadContext carries registered REST headers into the transport action handler.
            return threadContext.getHeader(headerName);
        }
    }

    /**
     * Always routes every request to the same partition. Useful for testing or for
     * single-partition deployments where partitioning is used only for the delay
     * (reject-delay) behaviour.
     * <p>
     * Configuration: {@code partition.resolver.fixed.partition = premium}
     */
    final class FixedPartitionResolver implements PartitionResolver {
        private final String partition;

        FixedPartitionResolver(Settings config) {
            this.partition = config.get("partition", "default");
        }

        @Override
        public String resolve(SearchRequestContext ctx) {
            return partition;
        }
    }

    /**
     * Routes a search to a partition by its shape: requests carrying aggregations go to the
     * {@code aggregation} partition, all other searches go to the {@code filter} partition.
     * Inspects the {@link SearchRequest} directly on the coordinator — no client header required.
     * <p>
     * A request that has both a query and aggregations is classed as {@code aggregation}
     * (aggregations dominate cost). Non-search requests (e.g. {@code _msearch}, or a non-search
     * action) resolve to {@code null} → {@code unknownPartition}.
     * <p>
     * Configuration:
     * <pre>
     *   partition.resolver.bySearchType.aggregation = aggregation   # partition for agg searches
     *   partition.resolver.bySearchType.filter      = filter        # partition for non-agg searches
     * </pre>
     */
    final class BySearchTypePartitionResolver implements PartitionResolver {
        private final String aggregationPartition;
        private final String filterPartition;

        BySearchTypePartitionResolver(Settings config) {
            this.aggregationPartition = config.get("aggregation", "aggregation");
            this.filterPartition = config.get("filter", "filter");
        }

        @Override
        public String resolve(SearchRequestContext ctx) {
            ActionRequest req = ctx.request();
            if (!(req instanceof SearchRequest)) {
                return null;  // not a search → unknownPartition
            }
            SearchSourceBuilder source = ((SearchRequest) req).source();
            boolean hasAggs = source != null && source.aggregations() != null && source.aggregations().count() > 0;
            return hasAggs ? aggregationPartition : filterPartition;
        }
    }
}
