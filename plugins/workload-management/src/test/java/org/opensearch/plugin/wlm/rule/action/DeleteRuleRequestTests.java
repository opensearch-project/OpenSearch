/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.plugin.wlm.RuleTestUtils._ID_ONE;

public class DeleteRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test serialization and deserialization of DeleteRuleRequest
     */
    public void testSerialization() throws IOException {
        DeleteRuleRequest request = new DeleteRuleRequest(_ID_ONE);
        assertEquals(_ID_ONE, request.getId());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        DeleteRuleRequest deserialized = new DeleteRuleRequest(in);

        assertEquals(request.getId(), deserialized.getId());
    }

    /**
     * Test validation when ID is null
     */
    public void testValidationFailsWhenIdIsNull() {
        DeleteRuleRequest request = new DeleteRuleRequest((String) null);
        ActionRequestValidationException exception = request.validate();
        assertNotNull(exception);
        assertTrue(exception.validationErrors().contains("id is missing"));
    }
}
