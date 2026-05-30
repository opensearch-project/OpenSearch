/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.be.lucene.serializers.AbstractQuerySerializer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Lucene-as-driver {@link FragmentConvertor}. Walks the resolved fragment, finds the
 * {@link OpenSearchFilter}, and serializes its condition as a {@link BoolQueryBuilder}'s
 * NamedWriteable bytes. Empty bytes when the fragment has no filter ({@code count(*)} over
 * MatchAllDocs at the data node).
 *
 * <p>Reuses the same leaf-serializer registry as {@link LuceneSubtreeConvertor} via
 * {@link QuerySerializerRegistry} — keyword equality, MATCH, MATCH_PHRASE, etc. all
 * round-trip through the same {@link DelegatedPredicateSerializer} → {@link QueryBuilder}
 * path. The data-node Lucene driver deserializes the bytes via NamedWriteable and runs
 * {@code IndexSearcher.count} on the resulting {@link QueryBuilder#toQuery(QueryShardContext)}.
 *
 * <p>Multi-stage / non-shard-scan fragments aren't supported: Lucene drives shard-local
 * count fragments only. Reduce or coordinator stages still run on DataFusion, so this
 * convertor is invoked only when the planner picked Lucene as the StagePlan's backend —
 * which happens exclusively for count-fast-path-eligible shards today.
 *
 * @opensearch.internal
 */
final class LuceneFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFragmentConvertor.class);

    private final Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers;

    LuceneFragmentConvertor(Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers) {
        this.leafSerializers = leafSerializers;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        // Lucene-driver wire format: [columnNames StringCollection] [hasFilter boolean]
        // [QueryBuilder NamedWriteable]?. Both ends are controlled (this convertor on the
        // coordinator, LuceneScanInstructionHandler on the data node), so a tiny custom
        // format is fine — beats threading column names through the InstructionNode.
        List<String> columnNames = extractAggCallNames(fragment);
        QueryBuilder filterQuery = null;
        Filter filter = findFilter(fragment);
        if (filter != null) {
            // strip() in FragmentConversionDriver replaces OpenSearchFilter with a plain
            // LogicalFilter, so the field-storage info lives on the OpenSearch ancestor
            // below (the TableScan). Walk down past LogicalFilter to find the nearest
            // OpenSearchRelNode and use its output field storage. The condition itself was
            // already resolved (annotation placeholders unwrapped) by the resolver in strip().
            List<FieldStorageInfo> fieldStorage = findFieldStorage(filter);
            filterQuery = toQueryBuilder(filter.getCondition(), fieldStorage);
        }
        byte[] bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(columnNames);
            if (filterQuery == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeNamedWriteable(filterQuery);
            }
            bytes = BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene-driver fragment", e);
        }
        LOGGER.debug("[lucene-count] convertFragment columnNames={} filterQuery={} bytes={}", columnNames, filterQuery, bytes.length);
        return bytes;
    }

    /**
     * Walks down to find an Aggregate (Calcite {@link Aggregate} or {@code OpenSearchAggregate})
     * and extracts the user-facing call names. These become the Arrow output column names so
     * the coordinator's reduce sink sees the schema it expects.
     */
    private static List<String> extractAggCallNames(RelNode root) {
        RelNode current = root;
        while (current != null) {
            if (current instanceof Aggregate agg) {
                List<String> names = new ArrayList<>(agg.getAggCallList().size());
                for (AggregateCall call : agg.getAggCallList()) {
                    names.add(call.getName());
                }
                return names;
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        return List.of();
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        // Lucene-as-driver count fragments don't go through the partial-agg-on-top split —
        // the count answer is one number per shard, no DataFusion-side aggregation step.
        // The wire bytes (see convertFragment) already carry the aggregate-call column
        // names so the Lucene driver builds the partial-shaped Arrow batch directly.
        return innerBytes;
    }

    /**
     * Walks the linear input chain looking for any Calcite {@link Filter} (covers both
     * {@link OpenSearchFilter} and the plain {@code LogicalFilter} that
     * {@code FragmentConversionDriver.strip} produces once annotation resolution unwraps the
     * filter's condition into native predicate calls).
     */
    private static Filter findFilter(RelNode node) {
        RelNode current = node;
        while (current != null) {
            if (current instanceof Filter filter) return filter;
            if (current.getInputs().isEmpty()) return null;
            current = current.getInputs().getFirst();
        }
        return null;
    }

    /**
     * Returns the field-storage info for a filter's child operator. When the filter is a
     * native {@link OpenSearchFilter} this is just its own {@code getOutputFieldStorage()};
     * for a plain {@code LogicalFilter} produced by {@code strip()}, walk the input chain to
     * the nearest {@link OpenSearchRelNode} (the TableScan) and use its storage. Per-leaf
     * serializers consult this list to resolve column references back to their backing fields.
     */
    private static List<FieldStorageInfo> findFieldStorage(Filter filter) {
        if (filter instanceof OpenSearchFilter osf) {
            return osf.getOutputFieldStorage();
        }
        RelNode current = filter.getInput();
        while (current != null) {
            if (current instanceof OpenSearchRelNode osNode) {
                return osNode.getOutputFieldStorage();
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        return List.of();
    }

    /**
     * Recursively converts a filter condition RexNode to a {@link QueryBuilder}. Mirrors
     * {@link LuceneSubtreeConvertor#toQueryBuilder} — same boolean structure handling
     * (AND→MUST, OR→SHOULD, NOT→MUST_NOT), same per-leaf serializer lookup. The duplication
     * is intentional: the delegation flow operates on a {@code DelegatedSubtreeConvertor}
     * SPI typed for serialized-bytes output, while the driver flow operates on
     * {@link FragmentConvertor} typed for whole-fragment serialization. Sharing the leaf
     * logic via a shared helper would be a follow-up cleanup.
     */
    private QueryBuilder toQueryBuilder(RexNode node, List<FieldStorageInfo> fieldStorage) {
        if (node instanceof AnnotatedPredicate ap) {
            node = ap.unwrap();
        }
        if (node instanceof RexCall call) {
            switch (call.getKind()) {
                case AND: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.must(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case OR: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.should(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case NOT: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    b.mustNot(toQueryBuilder(call.getOperands().get(0), fieldStorage));
                    return b;
                }
                default:
                    return leafToQueryBuilder(call, fieldStorage);
            }
        }
        throw new IllegalStateException("Unexpected RexNode in Lucene-driver filter condition: " + node);
    }

    private QueryBuilder leafToQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ScalarFunction fn = ScalarFunction.fromSqlOperatorWithFallback(call.getOperator());
        if (fn == null) {
            throw new IllegalStateException("Unrecognized operator in Lucene-driver filter: " + call.getOperator());
        }
        DelegatedPredicateSerializer serializer = leafSerializers.get(fn);
        if (serializer == null) {
            throw new IllegalStateException("No Lucene serializer for [" + fn + "] in driver-mode filter");
        }
        return ((AbstractQuerySerializer) serializer).buildQueryBuilder(call, fieldStorage);
    }
}
