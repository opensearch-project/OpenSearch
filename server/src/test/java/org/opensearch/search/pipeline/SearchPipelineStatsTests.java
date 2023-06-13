/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
            new SearchPipelineStats.Stats(1, 2, 3, 4),
            new SearchPipelineStats.Stats(5, 6, 7, 8),
            List.of(
                new SearchPipelineStats.PipelineStats(
                    "p1",
                    new SearchPipelineStats.Stats(9, 10, 11, 12),
                    new SearchPipelineStats.Stats(13, 14, 15, 16)
                ),
                new SearchPipelineStats.PipelineStats(
                    "p2",
                    new SearchPipelineStats.Stats(17, 18, 19, 20),
                    new SearchPipelineStats.Stats(21, 22, 23, 24)
                )

            ),
            Map.of(
                "p1",
                new SearchPipelineStats.PipelineDetailStats(
                    List.of(new SearchPipelineStats.ProcessorStats("req1:a", "req1", new SearchPipelineStats.Stats(25, 26, 27, 28))),
                    List.of(new SearchPipelineStats.ProcessorStats("rsp1:a", "rsp1", new SearchPipelineStats.Stats(29, 30, 31, 32)))
                ),
                "p2",
                new SearchPipelineStats.PipelineDetailStats(
                    List.of(
                        new SearchPipelineStats.ProcessorStats("req1:a", "req1", new SearchPipelineStats.Stats(33, 34, 35, 36)),
                        new SearchPipelineStats.ProcessorStats("req2", "req2", new SearchPipelineStats.Stats(37, 38, 39, 40))
                    ),
                    List.of()
                )
            )
        );
    }

    public void testToXContent() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XContentBuilder xContentBuilder = new XContentBuilder(JsonXContent.jsonXContent, bos);
        xContentBuilder.prettyPrint();
        xContentBuilder.startObject();
        createStats().toXContent(xContentBuilder, null);
        xContentBuilder.endObject();
        xContentBuilder.close();

        String jsonContent = bos.toString(StandardCharsets.UTF_8);

        assertEquals(
            "{\n"
                + "  \"search_pipeline\" : {\n"
                + "    \"total_request\" : {\n"
                + "      \"count\" : 1,\n"
                + "      \"time_in_millis\" : 2,\n"
                + "      \"current\" : 3,\n"
                + "      \"failed\" : 4\n"
                + "    },\n"
                + "    \"total_response\" : {\n"
                + "      \"count\" : 5,\n"
                + "      \"time_in_millis\" : 6,\n"
                + "      \"current\" : 7,\n"
                + "      \"failed\" : 8\n"
                + "    },\n"
                + "    \"pipelines\" : {\n"
                + "      \"p1\" : {\n"
                + "        \"request\" : {\n"
                + "          \"count\" : 9,\n"
                + "          \"time_in_millis\" : 10,\n"
                + "          \"current\" : 11,\n"
                + "          \"failed\" : 12\n"
                + "        },\n"
                + "        \"response\" : {\n"
                + "          \"count\" : 13,\n"
                + "          \"time_in_millis\" : 14,\n"
                + "          \"current\" : 15,\n"
                + "          \"failed\" : 16\n"
                + "        },\n"
                + "        \"request_processors\" : [\n"
                + "          {\n"
                + "            \"req1:a\" : {\n"
                + "              \"type\" : \"req1\",\n"
                + "              \"stats\" : {\n"
                + "                \"count\" : 25,\n"
                + "                \"time_in_millis\" : 26,\n"
                + "                \"current\" : 27,\n"
                + "                \"failed\" : 28\n"
                + "              }\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"response_processors\" : [\n"
                + "          {\n"
                + "            \"rsp1:a\" : {\n"
                + "              \"type\" : \"rsp1\",\n"
                + "              \"stats\" : {\n"
                + "                \"count\" : 29,\n"
                + "                \"time_in_millis\" : 30,\n"
                + "                \"current\" : 31,\n"
                + "                \"failed\" : 32\n"
                + "              }\n"
                + "            }\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"p2\" : {\n"
                + "        \"request\" : {\n"
                + "          \"count\" : 17,\n"
                + "          \"time_in_millis\" : 18,\n"
                + "          \"current\" : 19,\n"
                + "          \"failed\" : 20\n"
                + "        },\n"
                + "        \"response\" : {\n"
                + "          \"count\" : 21,\n"
                + "          \"time_in_millis\" : 22,\n"
                + "          \"current\" : 23,\n"
                + "          \"failed\" : 24\n"
                + "        },\n"
                + "        \"request_processors\" : [\n"
                + "          {\n"
                + "            \"req1:a\" : {\n"
                + "              \"type\" : \"req1\",\n"
                + "              \"stats\" : {\n"
                + "                \"count\" : 33,\n"
                + "                \"time_in_millis\" : 34,\n"
                + "                \"current\" : 35,\n"
                + "                \"failed\" : 36\n"
                + "              }\n"
                + "            }\n"
                + "          },\n"
                + "          {\n"
                + "            \"req2\" : {\n"
                + "              \"type\" : \"req2\",\n"
                + "              \"stats\" : {\n"
                + "                \"count\" : 37,\n"
                + "                \"time_in_millis\" : 38,\n"
                + "                \"current\" : 39,\n"
                + "                \"failed\" : 40\n"
                + "              }\n"
                + "            }\n"
                + "          }\n"
                + "        ],\n"
                + "        \"response_processors\" : [ ]\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}",
            jsonContent
        );
    }
}
