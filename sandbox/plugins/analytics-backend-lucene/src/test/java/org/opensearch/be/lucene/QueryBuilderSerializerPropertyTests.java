/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;

import org.opensearch.be.lucene.predicate.QueryBuilderSerializer;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link QueryBuilderSerializer}.
 */
class QueryBuilderSerializerPropertyTests {

    @Property(tries = 100)
    // Feature: lucene-backend-plugin, Property 5: QueryBuilder serialization round-trip
    void serializationRoundTrip(
        @ForAll @AlphaChars @StringLength(min = 1, max = 50) String fieldName,
        @ForAll @StringLength(min = 0, max = 100) String value
    ) {
        TermQueryBuilder original = new TermQueryBuilder(fieldName, value);

        byte[] serialized = QueryBuilderSerializer.serialize(original);
        QueryBuilder deserialized = QueryBuilderSerializer.deserialize(serialized);

        assertThat(deserialized).isInstanceOf(TermQueryBuilder.class);
        TermQueryBuilder roundTripped = (TermQueryBuilder) deserialized;
        assertThat(roundTripped.fieldName()).isEqualTo(original.fieldName());
        assertThat(roundTripped.value()).isEqualTo(original.value());
    }
}
