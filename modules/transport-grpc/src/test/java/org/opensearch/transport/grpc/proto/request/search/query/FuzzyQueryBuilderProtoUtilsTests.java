/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.search.FuzzyQuery;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.test.OpenSearchTestCase;

public class FuzzyQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithBasicFuzzyQuery() {
        FieldValue value = FieldValue.newBuilder().setString("jonh").build();
        org.opensearch.protobufs.FuzzyQuery fuzzyQuery = org.opensearch.protobufs.FuzzyQuery.newBuilder()
            .setField("name")
            .setValue(value)
            .build();

        FuzzyQueryBuilder fuzzyQueryBuilder = FuzzyQueryBuilderProtoUtils.fromProto(fuzzyQuery);

        assertNotNull("FuzzyQueryBuilder should not be null", fuzzyQueryBuilder);
        assertEquals("Field name should match", "name", fuzzyQueryBuilder.fieldName());
        assertEquals("Value should match", "jonh", fuzzyQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, fuzzyQueryBuilder.boost(), 0.0f);
        assertEquals("Default fuzziness should match", FuzzyQueryBuilder.DEFAULT_FUZZINESS, fuzzyQueryBuilder.fuzziness());
        assertEquals("Default prefix length should match", FuzzyQuery.defaultPrefixLength, fuzzyQueryBuilder.prefixLength());
        assertEquals("Default max expansions should match", FuzzyQuery.defaultMaxExpansions, fuzzyQueryBuilder.maxExpansions());
        assertEquals("Default transpositions should match", FuzzyQuery.defaultTranspositions, fuzzyQueryBuilder.transpositions());
    }

    public void testFromProtoWithAllParameters() {
        FieldValue value = FieldValue.newBuilder().setString("jonh").build();
        org.opensearch.protobufs.Fuzziness fuzziness = org.opensearch.protobufs.Fuzziness.newBuilder().setString("AUTO").build();

        org.opensearch.protobufs.FuzzyQuery fuzzyQuery = org.opensearch.protobufs.FuzzyQuery.newBuilder()
            .setField("name")
            .setValue(value)
            .setFuzziness(fuzziness)
            .setPrefixLength(2)
            .setMaxExpansions(100)
            .setTranspositions(false)
            .setRewrite("constant_score")
            .setBoost(1.5f)
            .setXName("test_query")
            .build();

        FuzzyQueryBuilder fuzzyQueryBuilder = FuzzyQueryBuilderProtoUtils.fromProto(fuzzyQuery);

        assertNotNull("FuzzyQueryBuilder should not be null", fuzzyQueryBuilder);
        assertEquals("Field name should match", "name", fuzzyQueryBuilder.fieldName());
        assertEquals("Value should match", "jonh", fuzzyQueryBuilder.value());
        assertEquals("Fuzziness should match", Fuzziness.AUTO, fuzzyQueryBuilder.fuzziness());
        assertEquals("Prefix length should match", 2, fuzzyQueryBuilder.prefixLength());
        assertEquals("Max expansions should match", 100, fuzzyQueryBuilder.maxExpansions());
        assertEquals("Transpositions should match", false, fuzzyQueryBuilder.transpositions());
        assertEquals("Rewrite should match", "constant_score", fuzzyQueryBuilder.rewrite());
        assertEquals("Boost should match", 1.5f, fuzzyQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_query", fuzzyQueryBuilder.queryName());
    }

    public void testFromProtoWithFuzzinessInt() {
        FieldValue value = FieldValue.newBuilder().setString("jonh").build();
        org.opensearch.protobufs.Fuzziness fuzziness = org.opensearch.protobufs.Fuzziness.newBuilder().setInt32(2).build();

        org.opensearch.protobufs.FuzzyQuery fuzzyQuery = org.opensearch.protobufs.FuzzyQuery.newBuilder()
            .setField("name")
            .setValue(value)
            .setFuzziness(fuzziness)
            .build();

        FuzzyQueryBuilder fuzzyQueryBuilder = FuzzyQueryBuilderProtoUtils.fromProto(fuzzyQuery);

        assertNotNull("FuzzyQueryBuilder should not be null", fuzzyQueryBuilder);
        assertEquals("Fuzziness should match", Fuzziness.fromEdits(2), fuzzyQueryBuilder.fuzziness());
    }

    public void testFromProtoWithTranspositionsTrue() {
        FieldValue value = FieldValue.newBuilder().setString("jonh").build();
        org.opensearch.protobufs.FuzzyQuery fuzzyQuery = org.opensearch.protobufs.FuzzyQuery.newBuilder()
            .setField("name")
            .setValue(value)
            .setTranspositions(true)
            .build();

        FuzzyQueryBuilder fuzzyQueryBuilder = FuzzyQueryBuilderProtoUtils.fromProto(fuzzyQuery);

        assertNotNull("FuzzyQueryBuilder should not be null", fuzzyQueryBuilder);
        assertEquals("Transpositions should match", true, fuzzyQueryBuilder.transpositions());
    }

    public void testFromProtoWithMinimalFields() {
        FieldValue value = FieldValue.newBuilder().setString("text").build();
        org.opensearch.protobufs.FuzzyQuery fuzzyQuery = org.opensearch.protobufs.FuzzyQuery.newBuilder()
            .setField("description")
            .setValue(value)
            .build();

        FuzzyQueryBuilder fuzzyQueryBuilder = FuzzyQueryBuilderProtoUtils.fromProto(fuzzyQuery);

        assertNotNull("FuzzyQueryBuilder should not be null", fuzzyQueryBuilder);
        assertEquals("Field name should match", "description", fuzzyQueryBuilder.fieldName());
        assertEquals("Value should match", "text", fuzzyQueryBuilder.value());
        assertNull("Query name should be null", fuzzyQueryBuilder.queryName());
    }
}
