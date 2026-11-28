/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PitUsageResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        PitUsageResponse response = createTestItem();
        PitUsageResponse deserialized = copyWriteable(response, new NamedWriteableRegistry(Collections.emptyList()), PitUsageResponse::new);
        assertNotSame(response, deserialized);
        assertEquals(response.getPitUsages(), deserialized.getPitUsages());
        assertEquals(response.getTotalShards(), deserialized.getTotalShards());
        assertEquals(response.getSuccessfulShards(), deserialized.getSuccessfulShards());
        assertEquals(response.getFailedShards(), deserialized.getFailedShards());
    }

    public void testToXContent() {
        PitUsageResponse response = new PitUsageResponse(
            Collections.singletonList(new PitUsageInfo("pit1", 2, 100)),
            2,
            2,
            0,
            Collections.emptyList()
        );
        assertEquals(
            "{\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},"
                + "\"pits\":[{\"pit_id\":\"pit1\",\"search_requests\":2,\"hits_returned\":100}]}",
            Strings.toString(MediaTypeRegistry.JSON, response)
        );
    }

    private PitUsageResponse createTestItem() {
        int numPits = randomInt(10);
        List<PitUsageInfo> pitUsages = new ArrayList<>(numPits);
        for (int i = 0; i < numPits; i++) {
            pitUsages.add(new PitUsageInfo(randomAlphaOfLength(10), randomNonNegativeLong(), randomNonNegativeLong()));
        }
        int totalShards = randomIntBetween(0, 10);
        int failedShards = randomIntBetween(0, totalShards);
        return new PitUsageResponse(pitUsages, totalShards, totalShards - failedShards, failedShards, Collections.emptyList());
    }
}
