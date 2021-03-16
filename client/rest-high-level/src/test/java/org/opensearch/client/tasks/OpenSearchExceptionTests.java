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
package org.opensearch.client.tasks;

import org.opensearch.OpenSearchException;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

public class OpenSearchExceptionTests extends AbstractResponseTestCase<OpenSearchException,
    org.opensearch.client.tasks.OpenSearchException> {

    @Override
    protected OpenSearchException createServerTestInstance(XContentType xContentType) {
        IllegalStateException ies = new IllegalStateException("illegal_state");
        IllegalArgumentException iae = new IllegalArgumentException("argument", ies);
        OpenSearchException exception = new OpenSearchException("elastic_exception", iae);
        exception.addHeader("key","value");
        exception.addMetadata("es.meta","data");
        exception.addSuppressed(new NumberFormatException("3/0"));
        return exception;
    }

    @Override
    protected OpenSearchException doParseToClientInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return OpenSearchException.fromXContent(parser);
    }

    @Override
    protected void assertInstances(OpenSearchException serverTestInstance, ElasticsearchException clientInstance) {

        IllegalArgumentException sCauseLevel1 = (IllegalArgumentException) serverTestInstance.getCause();
        ElasticsearchException cCauseLevel1 = clientInstance.getCause();

        assertTrue(sCauseLevel1 !=null);
        assertTrue(cCauseLevel1 !=null);

        IllegalStateException causeLevel2 = (IllegalStateException) serverTestInstance.getCause().getCause();
        ElasticsearchException cCauseLevel2 = clientInstance.getCause().getCause();
        assertTrue(causeLevel2 !=null);
        assertTrue(cCauseLevel2 !=null);


        OpenSearchException cause = new OpenSearchException(
            "OpenSearch exception [type=illegal_state_exception, reason=illegal_state]"
        );
        OpenSearchException caused1 = new OpenSearchException(
            "OpenSearch exception [type=illegal_argument_exception, reason=argument]",cause
        );
        OpenSearchException caused2 = new OpenSearchException(
            "OpenSearch exception [type=exception, reason=elastic_exception]",caused1
        );

        caused2.addHeader("key", Collections.singletonList("value"));
        OpenSearchException supp = new OpenSearchException(
            "OpenSearch exception [type=number_format_exception, reason=3/0]"
        );
        caused2.addSuppressed(Collections.singletonList(supp));

        assertEquals(caused2,clientInstance);

    }

}
