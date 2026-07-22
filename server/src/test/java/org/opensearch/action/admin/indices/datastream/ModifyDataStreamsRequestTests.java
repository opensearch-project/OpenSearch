/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.datastream.ModifyDataStreamsAction.Request;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ModifyDataStreamsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        int numActions = randomIntBetween(1, 4);
        List<DataStreamAction> actions = new ArrayList<>(numActions);
        for (int i = 0; i < numActions; i++) {
            String dataStream = randomAlphaOfLength(6);
            String index = randomAlphaOfLength(6);
            if (randomBoolean()) {
                actions.add(DataStreamAction.addBackingIndex(dataStream, index));
            } else {
                actions.add(DataStreamAction.removeBackingIndex(dataStream, index));
            }
        }
        return new Request(actions);
    }

    public void testValidateRequest() {
        Request req = new Request(List.of(DataStreamAction.addBackingIndex("my-data-stream", "my-index")));
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testValidateRequestWithoutActions() {
        Request req = new Request(List.of());
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("must specify at least one data stream modification action"));
    }

    public void testIndicesResolveToBackingIndices() {
        Request req = new Request(
            List.of(
                DataStreamAction.addBackingIndex("my-data-stream", "index-1"),
                DataStreamAction.removeBackingIndex("my-data-stream", "index-2")
            )
        );
        assertThat(req.indices(), equalTo(new String[] { "index-1", "index-2" }));
        assertTrue(req.includeDataStreams());
    }
}
