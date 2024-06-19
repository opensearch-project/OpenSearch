/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.cluster.metadata.QueryGroup.QueryGroupMode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MONITOR;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.TIMESTAMP_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.compareResourceLimits;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;

public class CreateQueryGroupRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        CreateQueryGroupRequest request = new CreateQueryGroupRequest(queryGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateQueryGroupRequest otherRequest = new CreateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getMode(), otherRequest.getMode());
        compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }

    public void testInvalidName() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest("", _ID_ONE, QueryGroupMode.fromName(MONITOR), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest("-test", _ID_ONE, QueryGroupMode.fromName(MONITOR), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest(":test", _ID_ONE, QueryGroupMode.fromName(MONITOR), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
    }

    public void testInvalidResourceLimitList() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest(NAME_ONE, _ID_ONE, QueryGroupMode.fromName(MONITOR), new HashMap<>(), TIMESTAMP_ONE)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest(
                NAME_ONE,
                _ID_ONE,
                QueryGroupMode.fromName(MONITOR),
                Map.of("jvm", 0.3, "jvm", 0.4),
                TIMESTAMP_ONE
            )
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest(NAME_ONE, _ID_ONE, QueryGroupMode.fromName(MONITOR), Map.of("jvm", -0.3), TIMESTAMP_ONE)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest(NAME_ONE, _ID_ONE, QueryGroupMode.fromName(MONITOR), Map.of("jvm", 12.0), TIMESTAMP_ONE)
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest(NAME_ONE, _ID_ONE, QueryGroupMode.fromName(MONITOR), Map.of("cpu", 0.3), TIMESTAMP_ONE)
        );
    }

    public void testInvalidEnforcement() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new CreateQueryGroupRequest(NAME_ONE, _ID_ONE, QueryGroupMode.fromName("random"), Map.of("jvm", 0.3), TIMESTAMP_ONE)
        );
    }
}
