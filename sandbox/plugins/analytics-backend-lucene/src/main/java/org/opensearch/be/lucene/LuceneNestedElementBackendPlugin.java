/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.search.IndexSearcher;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Analytics backend for the Engine-4 <em>element index</em> ({@code aux__lucene__nested}). It is
 * <b>accept-only</b>: it declares FILTER capabilities for nested string-leaf columns over the element
 * format and accepts FILTER delegation, so a nested-leaf predicate ({@code attributes.key='…'}) routes
 * here (the field's only viable filter backend, see {@code FieldStorageResolver}). It never drives a
 * scan. Its {@link #getFilterDelegationHandle} runs the query on the co-located element index and maps
 * matches back to parent rows via {@code __parent_row__} (see {@link NestedElementFilterDelegationHandle}).
 *
 * <p>Registered as a second {@code AnalyticsSearchBackendPlugin} SPI extension alongside
 * {@link LuceneAnalyticsBackendPlugin}; constructed with the providing {@link LucenePlugin}.
 *
 * @opensearch.internal
 */
public class LuceneNestedElementBackendPlugin implements AnalyticsSearchBackendPlugin {

    /** The element index format; also this backend's name, so nested-leaf fields route here by format name. */
    private static final DataFormat ELEMENT_FORMAT = LucenePlugin.NESTED_CHILD_DATA_FORMAT;
    private static final Set<String> ELEMENT_FORMATS = Set.of(ELEMENT_FORMAT.name());

    private static final Set<ScalarFunction> STANDARD_OPS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        ScalarFunction.LIKE,
        ScalarFunction.SARG_PREDICATE
    );

    private static final Set<FieldType> STANDARD_TYPES = new HashSet<>();
    static {
        STANDARD_TYPES.add(FieldType.KEYWORD);
        STANDARD_TYPES.add(FieldType.TEXT);
        STANDARD_TYPES.add(FieldType.MATCH_ONLY_TEXT);
    }

    private static final Set<FieldType> KEYWORD_ONLY = Set.of(FieldType.KEYWORD);

    private static final Set<FilterCapability> FILTER_CAPS;
    static {
        Set<FilterCapability> caps = new HashSet<>();
        for (ScalarFunction op : STANDARD_OPS) {
            Set<FieldType> types = op == ScalarFunction.LIKE ? KEYWORD_ONLY : STANDARD_TYPES;
            caps.add(new FilterCapability.Standard(op, types, ELEMENT_FORMATS));
        }
        FILTER_CAPS = caps;
    }

    private final LucenePlugin plugin;

    public LuceneNestedElementBackendPlugin(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return ELEMENT_FORMAT.name();
    }

    @Override
    public BackendCapabilityProvider getCapabilityProvider() {
        return new BackendCapabilityProvider() {
            @Override
            public Set<EngineCapability> supportedEngineCapabilities() {
                return Set.of();
            }

            @Override
            public Set<FilterCapability> filterCapabilities() {
                return FILTER_CAPS;
            }

            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return QuerySerializerRegistry.getSerializers();
            }
        };
    }

    @Override
    public FilterDelegationHandle getFilterDelegationHandle(List<DelegatedExpression> expressions, CommonExecutionContext ctx) {
        ShardScanExecutionContext shardCtx = (ShardScanExecutionContext) ctx;
        IndexReaderProvider.Reader reader = shardCtx.getReader();
        LuceneReader elementReader = reader.getReader(ELEMENT_FORMAT, LuceneReader.class);
        if (elementReader == null) {
            throw new IllegalStateException(
                "No element-index reader available for format [" + ELEMENT_FORMAT.name() + "] on this shard; nested filter cannot run"
            );
        }
        // No query cache for the element searcher. The element index is a standalone DirectoryReader
        // opened on the aux directory (LuceneAuxReaderManager), NOT registered with the shard, so the
        // shard's IndicesQueryCache cannot resolve a shard id from its segment cores — ShardCoreKeyMap
        // throws "Could not extract shard id" the moment CachingWeightWrapper.scorerSupplier runs.
        // Element-index scans are cheap and short-lived, so skipping the cache is also fine on cost.
        IndexSearcher searcher = elementReader.searcher(null, null);
        QueryShardContext queryShardContext = LuceneAnalyticsBackendPlugin.buildMinimalQueryShardContext(shardCtx, searcher);
        BooleanSupplier isCancelled = () -> {
            Task task = shardCtx.getTask();
            return task instanceof CancellableTask ct && ct.isCancelled();
        };
        return new NestedElementFilterDelegationHandle(
            expressions,
            queryShardContext,
            elementReader,
            shardCtx.getNamedWriteableRegistry(),
            isCancelled
        );
    }

    @Override
    public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
        return QuerySerializerRegistry.getSerializers();
    }

    @Override
    public DelegatedSubtreeConvertor getDelegatedSubtreeConvertor() {
        return new LuceneSubtreeConvertor(QuerySerializerRegistry.getSerializers());
    }
}
