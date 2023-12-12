/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client.tasks;

import org.opensearch.client.AbstractResponseTestCase;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

public class OpenSearchExceptionTests extends AbstractResponseTestCase<
    org.opensearch.OpenSearchException,
    org.opensearch.client.tasks.OpenSearchException> {

    @Override
    protected org.opensearch.OpenSearchException createServerTestInstance(XContentType xContentType) {
        IllegalStateException ies = new IllegalStateException("illegal_state");
        IllegalArgumentException iae = new IllegalArgumentException("argument", ies);
        org.opensearch.OpenSearchException exception = new org.opensearch.OpenSearchException("elastic_exception", iae);
        exception.addHeader("key", "value");
        exception.addMetadata("opensearch.meta", "data");
        exception.addSuppressed(new NumberFormatException("3/0"));
        return exception;
    }

    @Override
    protected OpenSearchException doParseToClientInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return OpenSearchException.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.opensearch.OpenSearchException serverTestInstance, OpenSearchException clientInstance) {

        IllegalArgumentException sCauseLevel1 = (IllegalArgumentException) serverTestInstance.getCause();
        OpenSearchException cCauseLevel1 = clientInstance.getCause();

        assertTrue(sCauseLevel1 != null);
        assertTrue(cCauseLevel1 != null);

        IllegalStateException causeLevel2 = (IllegalStateException) serverTestInstance.getCause().getCause();
        OpenSearchException cCauseLevel2 = clientInstance.getCause().getCause();
        assertTrue(causeLevel2 != null);
        assertTrue(cCauseLevel2 != null);

        OpenSearchException cause = new OpenSearchException("OpenSearch exception [type=illegal_state_exception, reason=illegal_state]");
        OpenSearchException caused1 = new OpenSearchException(
            "OpenSearch exception [type=illegal_argument_exception, reason=argument]",
            cause
        );
        OpenSearchException caused2 = new OpenSearchException("OpenSearch exception [type=exception, reason=elastic_exception]", caused1);

        caused2.addHeader("key", Collections.singletonList("value"));
        OpenSearchException supp = new OpenSearchException("OpenSearch exception [type=number_format_exception, reason=3/0]");
        caused2.addSuppressed(Collections.singletonList(supp));

        assertEquals(caused2, clientInstance);

    }

}
