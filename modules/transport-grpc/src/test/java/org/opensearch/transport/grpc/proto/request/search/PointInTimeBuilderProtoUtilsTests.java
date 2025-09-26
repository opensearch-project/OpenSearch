/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.protobufs.PointInTimeReference;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

public class PointInTimeBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithValidValues() {
        // Create a protobuf PointInTimeReference with valid values
        PointInTimeReference pointInTimeReference = PointInTimeReference.newBuilder().setId("test_pit_id").setKeepAlive("5m").build();

        // Call the method under test
        PointInTimeBuilder pointInTimeBuilder = PointInTimeBuilderProtoUtils.fromProto(pointInTimeReference);

        // Verify the result
        assertNotNull("PointInTimeBuilder should not be null", pointInTimeBuilder);
        assertEquals("ID should match", "test_pit_id", pointInTimeBuilder.getId());
        assertEquals("KeepAlive should match", TimeValue.timeValueMinutes(5), pointInTimeBuilder.getKeepAlive());
    }

    public void testFromProtoWithDifferentTimeFormats() {
        // Test with seconds
        PointInTimeReference pointInTimeReference1 = PointInTimeReference.newBuilder().setId("test_pit_id_1").setKeepAlive("30s").build();
        PointInTimeBuilder pointInTimeBuilder1 = PointInTimeBuilderProtoUtils.fromProto(pointInTimeReference1);
        assertEquals("KeepAlive should match for seconds", TimeValue.timeValueSeconds(30), pointInTimeBuilder1.getKeepAlive());

        // Test with hours
        PointInTimeReference pointInTimeReference2 = PointInTimeReference.newBuilder().setId("test_pit_id_2").setKeepAlive("2h").build();
        PointInTimeBuilder pointInTimeBuilder2 = PointInTimeBuilderProtoUtils.fromProto(pointInTimeReference2);
        assertEquals("KeepAlive should match for hours", TimeValue.timeValueHours(2), pointInTimeBuilder2.getKeepAlive());

        // Test with days
        PointInTimeReference pointInTimeReference3 = PointInTimeReference.newBuilder().setId("test_pit_id_3").setKeepAlive("1d").build();
        PointInTimeBuilder pointInTimeBuilder3 = PointInTimeBuilderProtoUtils.fromProto(pointInTimeReference3);
        assertEquals("KeepAlive should match for days", TimeValue.timeValueDays(1), pointInTimeBuilder3.getKeepAlive());

        // Test with milliseconds
        PointInTimeReference pointInTimeReference4 = PointInTimeReference.newBuilder().setId("test_pit_id_4").setKeepAlive("500ms").build();
        PointInTimeBuilder pointInTimeBuilder4 = PointInTimeBuilderProtoUtils.fromProto(pointInTimeReference4);
        assertEquals("KeepAlive should match for milliseconds", TimeValue.timeValueMillis(500), pointInTimeBuilder4.getKeepAlive());
    }

    public void testFromProtoWithComplexTimeFormat() {
        // Test with complex time format
        PointInTimeReference pointInTimeReference = PointInTimeReference.newBuilder().setId("test_pit_id").setKeepAlive("30m").build();
        PointInTimeBuilder pointInTimeBuilder = PointInTimeBuilderProtoUtils.fromProto(pointInTimeReference);

        // Calculate expected time value: 30m = 1800s
        TimeValue expectedTimeValue = new TimeValue(1800, TimeUnit.SECONDS);
        assertEquals("KeepAlive should match for complex format", expectedTimeValue, pointInTimeBuilder.getKeepAlive());
    }

    public void testFromProtoWithInvalidTimeFormat() {
        // Test with invalid time format
        PointInTimeReference pointInTimeReference = PointInTimeReference.newBuilder()
            .setId("test_pit_id")
            .setKeepAlive("invalid_time_format")
            .build();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PointInTimeBuilderProtoUtils.fromProto(pointInTimeReference)
        );

        assertTrue("Exception message should mention failed to parse", exception.getMessage().contains("failed to parse"));
    }
}
