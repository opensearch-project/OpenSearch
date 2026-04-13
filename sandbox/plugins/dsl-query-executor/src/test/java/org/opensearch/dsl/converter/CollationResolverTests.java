/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.junit.Test;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.search.aggregations.BucketOrder;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CollationResolverTests {

    @Test
    public void testResolveEmptyOrders() throws ConversionException {
        AggregationMetadata metadata = mock(AggregationMetadata.class);
        RelDataType postAggRowType = mock(RelDataType.class);

        when(metadata.getGroupings()).thenReturn(List.of());
        when(metadata.getBucketOrders()).thenReturn(List.of());

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, postAggRowType);

        assertNotNull(collations);
        assertEquals(0, collations.size());
    }

    @Test
    public void testResolveWithValidOrder() throws ConversionException {
        AggregationMetadata metadata = mock(AggregationMetadata.class);
        RelDataType postAggRowType = mock(RelDataType.class);
        GroupingInfo grouping = mock(GroupingInfo.class);

        when(grouping.getFieldNames()).thenReturn(List.of("category"));
        when(metadata.getGroupings()).thenReturn(List.of(grouping));
        when(metadata.getBucketOrders()).thenReturn(List.of(BucketOrder.key(true)));

        RelDataTypeField keyField = mock(RelDataTypeField.class);
        when(keyField.getIndex()).thenReturn(0);
        when(postAggRowType.getField("category", true, false)).thenReturn(keyField);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, postAggRowType);

        assertNotNull(collations);
    }

    @Test
    public void testResolveFieldNotFound() {
        AggregationMetadata metadata = mock(AggregationMetadata.class);
        RelDataType postAggRowType = mock(RelDataType.class);
        GroupingInfo grouping = mock(GroupingInfo.class);

        when(grouping.getFieldNames()).thenReturn(List.of("category"));
        when(metadata.getGroupings()).thenReturn(List.of(grouping));
        when(metadata.getBucketOrders()).thenReturn(List.of(BucketOrder.aggregation("missing", true)));
        when(postAggRowType.getField(anyString(), anyBoolean(), anyBoolean())).thenReturn(null);

        assertThrows(ConversionException.class,
            () -> CollationResolver.resolve(metadata, postAggRowType));
    }
}
