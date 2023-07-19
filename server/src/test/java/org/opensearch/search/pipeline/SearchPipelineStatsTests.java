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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
            )
        );
    }

    public void testToXContent() throws IOException {
        XContentBuilder actualBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        actualBuilder.startObject();
        createStats().toXContent(actualBuilder, null);
        actualBuilder.endObject();

        String expected = "{"
            + "  \"search_pipeline\" : {"
            + "    \"total_request\" : {"
            + "      \"count\" : 1,"
            + "      \"time_in_millis\" : 2,"
            + "      \"current\" : 3,"
            + "      \"failed\" : 4"
            + "    },"
            + "    \"total_response\" : {"
            + "      \"count\" : 5,"
            + "      \"time_in_millis\" : 6,"
            + "      \"current\" : 7,"
            + "      \"failed\" : 8"
            + "    },"
            + "    \"pipelines\" : {"
            + "      \"p1\" : {"
            + "        \"request\" : {"
            + "          \"count\" : 9,"
            + "          \"time_in_millis\" : 10,"
            + "          \"current\" : 11,"
            + "          \"failed\" : 12"
            + "        },"
            + "        \"response\" : {"
            + "          \"count\" : 13,"
            + "          \"time_in_millis\" : 14,"
            + "          \"current\" : 15,"
            + "          \"failed\" : 16"
            + "        },"
            + "        \"request_processors\" : ["
            + "          {"
            + "            \"req1:a\" : {"
            + "              \"type\" : \"req1\","
            + "              \"stats\" : {"
            + "                \"count\" : 25,"
            + "                \"time_in_millis\" : 26,"
            + "                \"current\" : 27,"
            + "                \"failed\" : 28"
            + "              }"
            + "            }"
            + "          }"
            + "        ],"
            + "        \"response_processors\" : ["
            + "          {"
            + "            \"rsp1:a\" : {"
            + "              \"type\" : \"rsp1\","
            + "              \"stats\" : {"
            + "                \"count\" : 29,"
            + "                \"time_in_millis\" : 30,"
            + "                \"current\" : 31,"
            + "                \"failed\" : 32"
            + "              }"
            + "            }"
            + "          }"
            + "        ]"
            + "      },"
            + "      \"p2\" : {"
            + "        \"request\" : {"
            + "          \"count\" : 17,"
            + "          \"time_in_millis\" : 18,"
            + "          \"current\" : 19,"
            + "          \"failed\" : 20"
            + "        },"
            + "        \"response\" : {"
            + "          \"count\" : 21,"
            + "          \"time_in_millis\" : 22,"
            + "          \"current\" : 23,"
            + "          \"failed\" : 24"
            + "        },"
            + "        \"request_processors\" : ["
            + "          {"
            + "            \"req1:a\" : {"
            + "              \"type\" : \"req1\","
            + "              \"stats\" : {"
            + "                \"count\" : 33,"
            + "                \"time_in_millis\" : 34,"
            + "                \"current\" : 35,"
            + "                \"failed\" : 36"
            + "              }"
            + "            }"
            + "          },"
            + "          {"
            + "            \"req2\" : {"
            + "              \"type\" : \"req2\","
            + "              \"stats\" : {"
            + "                \"count\" : 37,"
            + "                \"time_in_millis\" : 38,"
            + "                \"current\" : 39,"
            + "                \"failed\" : 40"
            + "              }"
            + "            }"
            + "          }"
            + "        ],"
            + "        \"response_processors\" : [ ]"
            + "      }"
            + "    }"
            + "  }"
            + "}";

        XContentParser expectedParser = JsonXContent.jsonXContent.createParser(
            this.xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            expected
        );
        XContentBuilder expectedBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        expectedBuilder.generator().copyCurrentStructure(expectedParser);

        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(expectedBuilder), false, (MediaType) XContentType.JSON),
            XContentHelper.convertToMap(BytesReference.bytes(actualBuilder), false, (MediaType) XContentType.JSON)
        );
    }
}
