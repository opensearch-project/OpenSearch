/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MONITOR;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.TIMESTAMP_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.compareResourceLimits;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;

public class UpdateQueryGroupRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(queryGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getMode(), otherRequest.getMode());
        compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }

    public void testSerializationOnlyName() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(NAME_ONE, null, new HashMap<>(), TIMESTAMP_ONE);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getMode(), otherRequest.getMode());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }

    public void testSerializationOnlyResourceLimit() throws IOException {
        UpdateQueryGroupRequest request = new UpdateQueryGroupRequest(NAME_ONE, null, Map.of("jvm", 0.4), TIMESTAMP_ONE);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateQueryGroupRequest otherRequest = new UpdateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getMode(), otherRequest.getMode());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }

    public void testInvalidName() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest("", QueryGroup.QueryGroupMode.fromName(MONITOR), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest("-test", QueryGroup.QueryGroupMode.fromName(MONITOR), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(":test", QueryGroup.QueryGroupMode.fromName(MONITOR), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
    }

    public void testInvalidResourceLimitList() {
        assertThrows(
            NullPointerException.class,
            () -> new UpdateQueryGroupRequest(NAME_ONE, QueryGroup.QueryGroupMode.fromName(MONITOR), null, TIMESTAMP_ONE)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(
                NAME_ONE,
                QueryGroup.QueryGroupMode.fromName(MONITOR),
                Map.of("jvm", 0.3, "jvm", 0.4),
                TIMESTAMP_ONE
            )
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(NAME_ONE, QueryGroup.QueryGroupMode.fromName(MONITOR), Map.of("jvm", -0.3), TIMESTAMP_ONE)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(NAME_ONE, QueryGroup.QueryGroupMode.fromName(MONITOR), Map.of("jvm", 12.0), TIMESTAMP_ONE)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(NAME_ONE, QueryGroup.QueryGroupMode.fromName(MONITOR), Map.of("cpu", 0.3), TIMESTAMP_ONE)
        );
    }

    public void testInvalidEnforcement() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateQueryGroupRequest(NAME_ONE, QueryGroup.QueryGroupMode.fromName("random"), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
    }
}
