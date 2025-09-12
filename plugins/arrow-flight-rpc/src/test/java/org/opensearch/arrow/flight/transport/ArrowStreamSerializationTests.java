/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class ArrowStreamSerializationTests extends OpenSearchTestCase {
    private NamedWriteableRegistry registry;
    private RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(StringTerms.class, StringTerms.NAME, StringTerms::new),
                new NamedWriteableRegistry.Entry(InternalAggregation.class, StringTerms.NAME, StringTerms::new),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), (si) -> DocValueFormat.RAW)
            )
        );
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        allocator.close();
    }

    public void testInternalAggregationSerializationDeserialization() throws IOException {
        StringTerms original = createTestStringTerms();

        try (VectorStreamOutput output = new VectorStreamOutput(allocator, Optional.empty())) {
            output.writeNamedWriteable(original);
            VectorSchemaRoot unifiedRoot = output.getRoot();

            try (VectorStreamInput input = new VectorStreamInput(unifiedRoot, registry)) {
                StringTerms deserialized = input.readNamedWriteable(StringTerms.class);
                assertEquals(String.valueOf(original), String.valueOf(deserialized));
            }
        }
    }

    private StringTerms createTestStringTerms() {
        return new StringTerms(
            "agg1",
            InternalOrder.key(true),
            InternalOrder.key(true),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            10,
            false,
            50,
            Arrays.asList(
                new StringTerms.Bucket(
                    new BytesRef("term1"),
                    100,
                    InternalAggregations.from(
                        Collections.singletonList(
                            new StringTerms(
                                "sub_agg_1",
                                InternalOrder.key(true),
                                InternalOrder.key(true),
                                Collections.emptyMap(),
                                DocValueFormat.RAW,
                                10,
                                false,
                                10,
                                Arrays.asList(
                                    new StringTerms.Bucket(
                                        new BytesRef("subterm1_1"),
                                        30,
                                        InternalAggregations.EMPTY,
                                        false,
                                        0,
                                        DocValueFormat.RAW
                                    )
                                ),
                                0,
                                new TermsAggregator.BucketCountThresholds(10, 0, 10, 10)
                            )
                        )
                    ),
                    false,
                    0,
                    DocValueFormat.RAW
                ),
                new StringTerms.Bucket(
                    new BytesRef("term2"),
                    100,
                    InternalAggregations.from(
                        Collections.singletonList(
                            new StringTerms(
                                "sub_agg_2",
                                InternalOrder.key(true),
                                InternalOrder.key(true),
                                Collections.emptyMap(),
                                DocValueFormat.RAW,
                                10,
                                false,
                                19,
                                Arrays.asList(
                                    new StringTerms.Bucket(
                                        new BytesRef("subterm2_1"),
                                        31,
                                        InternalAggregations.EMPTY,
                                        false,
                                        101,
                                        DocValueFormat.RAW
                                    )
                                ),
                                0,
                                new TermsAggregator.BucketCountThresholds(10, 0, 10, 10)
                            )
                        )
                    ),
                    false,
                    0,
                    DocValueFormat.RAW
                )
            ),
            0,
            new TermsAggregator.BucketCountThresholds(10, 0, 10, 10)
        );
    }
}
