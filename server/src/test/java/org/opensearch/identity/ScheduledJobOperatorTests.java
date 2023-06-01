/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.identity.schedule.ScheduledJobOperator;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;

public class ScheduledJobOperatorTests extends OpenSearchTestCase {

    public void testScheduledJobOperatorParseWithUser() throws Exception {
        XContentParser operatorParser = createParser(
            JsonXContent.jsonXContent,
            "{\"operator\":{\"user\": { \"username\":\"craig\", \"roles\": \"all_access\", \"backend_roles\": \"admin\"}}}"
        );
        ScheduledJobOperator operator = ScheduledJobOperator.parse(operatorParser);
        assertNotNull(operator.getIdentity());
        assertNotNull(operator.getIdentity().getUser());
        assertEquals("craig", operator.getIdentity().getUser().getUsername());
        assertEquals("all_access", operator.getIdentity().getUser().getAttributes().get("roles"));
    }

    public void testScheduledJobOperatorParseWithToken() throws Exception {
        XContentParser operatorParser = createParser(JsonXContent.jsonXContent, "{\"operator\":{\"token\": \"encodedJwt\"}}");
        ScheduledJobOperator operator = ScheduledJobOperator.parse(operatorParser);
        assertNotNull(operator.getIdentity());
        assertNotNull(operator.getIdentity().getAuthToken());
        assertEquals("encodedJwt", operator.getIdentity().getAuthToken());
    }

    public void testParseErrorOnNullUsername() throws Exception {
        XContentParser operatorParser = createParser(JsonXContent.jsonXContent, "{\"operator\":{\"user\": { \"username\": null}}}");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> ScheduledJobOperator.parse(operatorParser));
        assertThat(e.getMessage(), containsString("Can't get text on a VALUE_NULL"));
    }
}
