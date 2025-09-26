/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.common.breaker.ResponseLimitBreachedException;
import org.opensearch.common.breaker.ResponseLimitSettings;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class ResponseLimitBreachedExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testMetadataToProto() {
        // Create a ResponseLimitBreachedException with specific values
        int responseLimit = 10000; // Smaller value that fits in an int
        ResponseLimitSettings.LimitEntity limitEntity = ResponseLimitSettings.LimitEntity.INDICES;
        ResponseLimitBreachedException exception = new ResponseLimitBreachedException(
            "Test response limit breached",
            responseLimit,
            limitEntity
        );

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = ResponseLimitBreachedExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have response_limit field", metadata.containsKey("response_limit"));
        assertTrue("Should have limit_entity field", metadata.containsKey("limit_entity"));

        // Verify field values
        ObjectMap.Value responseLimitValue = metadata.get("response_limit");
        ObjectMap.Value limitEntityValue = metadata.get("limit_entity");

        assertEquals("response_limit should match", responseLimit, responseLimitValue.getInt32());
        assertEquals("limit_entity should match", limitEntity.toString(), limitEntityValue.getString());
    }
}
