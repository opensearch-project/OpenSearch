/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.core.common.breaker.CircuitBreaker.Durability;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class CircuitBreakingExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testMetadataToProto() {
        // Create a CircuitBreakingException with specific values
        long bytesWanted = 1024;
        long byteLimit = 512;
        Durability durability = Durability.TRANSIENT;
        CircuitBreakingException exception = new CircuitBreakingException("Test circuit breaking", bytesWanted, byteLimit, durability);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = CircuitBreakingExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have bytes_wanted field", metadata.containsKey("bytes_wanted"));
        assertTrue("Should have bytes_limit field", metadata.containsKey("bytes_limit"));
        assertTrue("Should have durability field", metadata.containsKey("durability"));

        // Verify field values
        ObjectMap.Value bytesWantedValue = metadata.get("bytes_wanted");
        ObjectMap.Value bytesLimitValue = metadata.get("bytes_limit");
        ObjectMap.Value durabilityValue = metadata.get("durability");

        assertEquals("bytes_wanted should match", bytesWanted, bytesWantedValue.getInt64());
        assertEquals("bytes_limit should match", byteLimit, bytesLimitValue.getInt64());
        assertEquals("durability should match", durability.toString(), durabilityValue.getString());
    }
}
