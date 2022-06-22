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

package org.opensearch.client;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.Script;
import org.opensearch.script.StoredScriptSource;

import java.util.Collections;
import java.util.Map;

import static org.opensearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class StoredScriptsIT extends OpenSearchRestHighLevelClientTestCase {

    private static final String id = "calculate-score";

    public void testGetStoredScript() throws Exception {
        final StoredScriptSource scriptSource = new StoredScriptSource(
            "painless",
            "Math.log(_score * 2) + params.my_modifier",
            Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType())
        );

        PutStoredScriptRequest request = new PutStoredScriptRequest(id, "score", new BytesArray("{}"), XContentType.JSON, scriptSource);
        assertAcked(execute(request, highLevelClient()::putScript, highLevelClient()::putScriptAsync));

        GetStoredScriptRequest getRequest = new GetStoredScriptRequest("calculate-score");
        getRequest.clusterManagerNodeTimeout("50s");

        GetStoredScriptResponse getResponse = execute(getRequest, highLevelClient()::getScript, highLevelClient()::getScriptAsync);

        assertThat(getResponse.getSource(), equalTo(scriptSource));
    }

    public void testDeleteStoredScript() throws Exception {
        final StoredScriptSource scriptSource = new StoredScriptSource(
            "painless",
            "Math.log(_score * 2) + params.my_modifier",
            Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType())
        );

        PutStoredScriptRequest request = new PutStoredScriptRequest(id, "score", new BytesArray("{}"), XContentType.JSON, scriptSource);
        assertAcked(execute(request, highLevelClient()::putScript, highLevelClient()::putScriptAsync));

        DeleteStoredScriptRequest deleteRequest = new DeleteStoredScriptRequest(id);
        deleteRequest.clusterManagerNodeTimeout("50s");
        deleteRequest.timeout("50s");
        assertAcked(execute(deleteRequest, highLevelClient()::deleteScript, highLevelClient()::deleteScriptAsync));

        GetStoredScriptRequest getRequest = new GetStoredScriptRequest(id);

        final OpenSearchStatusException statusException = expectThrows(
            OpenSearchStatusException.class,
            () -> execute(getRequest, highLevelClient()::getScript, highLevelClient()::getScriptAsync)
        );
        assertThat(statusException.status(), equalTo(RestStatus.NOT_FOUND));
    }

    public void testPutScript() throws Exception {
        final StoredScriptSource scriptSource = new StoredScriptSource(
            "painless",
            "Math.log(_score * 2) + params.my_modifier",
            Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType())
        );

        PutStoredScriptRequest request = new PutStoredScriptRequest(id, "score", new BytesArray("{}"), XContentType.JSON, scriptSource);
        assertAcked(execute(request, highLevelClient()::putScript, highLevelClient()::putScriptAsync));

        Map<String, Object> script = getAsMap("/_scripts/" + id);
        assertThat(extractValue("_id", script), equalTo(id));
        assertThat(extractValue("found", script), equalTo(true));
        assertThat(extractValue("script.lang", script), equalTo("painless"));
        assertThat(extractValue("script.source", script), equalTo("Math.log(_score * 2) + params.my_modifier"));
    }
}
