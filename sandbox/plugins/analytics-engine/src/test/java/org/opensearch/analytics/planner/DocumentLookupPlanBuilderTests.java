/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.opensearch.action.support.ReadAccessPolicy;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.query.QueryBuilderTranslationService;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentLookupPlanBuilderTests extends OpenSearchTestCase {

    public void testBuildAddsRowIdPredicateAndLookupMetadata() {
        SchemaPlus schema = Frameworks.createRootSchema(true);
        schema.add("idx", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder().add("tenant", SqlTypeName.VARCHAR).add("value", SqlTypeName.INTEGER).build();
            }
        });
        EngineContextProvider contextProvider = () -> new QueryRequestContext(null, schema);
        ReadAccessPolicy policy = mock(ReadAccessPolicy.class);
        ReadAccessPolicy.IndexGroup group = mock(ReadAccessPolicy.IndexGroup.class);
        when(policy.hasRestrictions()).thenReturn(true);
        when(policy.coveredConcreteIndices()).thenReturn(Set.of("idx"));
        when(policy.indexGroups()).thenReturn(List.of(group));
        when(group.concreteIndices()).thenReturn(Set.of("idx"));
        when(group.restrictions()).thenReturn(Optional.of(QueryBuilders.matchAllQuery()));

        RelNode result = new DocumentLookupPlanBuilder(contextProvider, rewriter()).build(new Index("idx", "uuid"), 17L, policy);

        assertTrue(result instanceof LogicalFilter);
        assertTrue(result.getRowType().getFieldNames().contains(DocumentInput.ROW_ID_FIELD));
        assertTrue(result.getRowType().getFieldNames().contains("_seq_no"));
        assertTrue(result.getRowType().getFieldNames().contains("_primary_term"));
        assertTrue(result.getRowType().getFieldNames().contains("_version"));
        assertEquals("=($2, 17)", ((LogicalFilter) result).getCondition().toString());
    }

    public void testBuildRejectsUnrestrictedPolicy() {
        DocumentLookupPlanBuilder builder = new DocumentLookupPlanBuilder(mock(EngineContextProvider.class), rewriter());
        expectThrows(IllegalArgumentException.class, () -> builder.build(new Index("idx", "uuid"), 1L, ReadAccessPolicy.unrestricted()));
    }

    private static LogicalPlanDlsRewriter rewriter() {
        QueryBuilderTranslationService translationService = new QueryBuilderTranslationService(
            List.of((query, cluster, table) -> cluster.getRexBuilder().makeLiteral(true))
        );
        return new LogicalPlanDlsRewriter(translationService);
    }
}
