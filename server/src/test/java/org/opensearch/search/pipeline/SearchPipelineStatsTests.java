/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.StreamsUtils.copyToStringFromClasspath;

public class SearchPipelineStatsTests extends OpenSearchTestCase {
    public void testSerializationRoundtrip() throws IOException {
        SearchPipelineStats stats = createStats();
        SearchPipelineStats deserialized;
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            stats.writeTo(bytesStreamOutput);
            try (StreamInput bytesStreamInput = bytesStreamOutput.bytes().streamInput()) {
                deserialized = new SearchPipelineStats(bytesStreamInput);
            }
        }
        assertEquals(stats, deserialized);
    }

    private static SearchPipelineStats createStats() {
        return new SearchPipelineStats(
            new OperationStats(1, 2, 3, 4),
            new OperationStats(5, 6, 7, 8),
            List.of(
                new SearchPipelineStats.PerPipelineStats("p1", new OperationStats(9, 10, 11, 12), new OperationStats(13, 14, 15, 16)),
                new SearchPipelineStats.PerPipelineStats("p2", new OperationStats(17, 18, 19, 20), new OperationStats(21, 22, 23, 24))
            ),
            Map.of(
                "p1",
                new SearchPipelineStats.PipelineDetailStats(
                    List.of(new SearchPipelineStats.ProcessorStats("req1:a", "req1", new OperationStats(25, 26, 27, 28))),
                    List.of(new SearchPipelineStats.ProcessorStats("rsp1:a", "rsp1", new OperationStats(29, 30, 31, 32)))
                ),
                "p2",
                new SearchPipelineStats.PipelineDetailStats(
                    List.of(
                        new SearchPipelineStats.ProcessorStats("req1:a", "req1", new OperationStats(33, 34, 35, 36)),
                        new SearchPipelineStats.ProcessorStats("req2", "req2", new OperationStats(37, 38, 39, 40))
                    ),
                    List.of()
                )
            ),
            new SearchPipelineStats.FactoryDetailStats(
                List.of(
                    new SearchPipelineStats.FactoryStats(
                        "reqFactory1",
                        new OperationStats(1, 1, 0, 0, TimeUnit.MICROSECONDS),
                        new OperationStats(1, 2, 0, 0, TimeUnit.MICROSECONDS)
                    )
                ),
                List.of(
                    new SearchPipelineStats.FactoryStats(
                        "resFactory1",
                        new OperationStats(1, 1, 0, 0, TimeUnit.MICROSECONDS),
                        new OperationStats(1, 2, 0, 0, TimeUnit.MICROSECONDS)
                    )
                )
            ),
            new SearchPipelineStats.PipelineDetailStats(
                List.of(new SearchPipelineStats.ProcessorStats("sysReq1", "sysReq1", new OperationStats(1, 1, 0, 0))),
                List.of(new SearchPipelineStats.ProcessorStats("sysRes1", "sysRes1", new OperationStats(1, 1, 0, 0)))
            )
        );
    }

    public void testToXContent() throws IOException {
        XContentBuilder actualBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        actualBuilder.startObject();
        createStats().toXContent(actualBuilder, null);
        actualBuilder.endObject();
        String expected = copyToStringFromClasspath("/org/opensearch/search/pipeline/expected-stats.json");

        XContentParser expectedParser = JsonXContent.jsonXContent.createParser(
            this.xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            expected
        );
        XContentBuilder expectedBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        expectedBuilder.generator().copyCurrentStructure(expectedParser);

        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(expectedBuilder), false, (MediaType) MediaTypeRegistry.JSON),
            XContentHelper.convertToMap(BytesReference.bytes(actualBuilder), false, (MediaType) MediaTypeRegistry.JSON)
        );
    }
}
