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

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.admin.indices.datastream.GetDataStreamAction.Request;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.List;

import static org.opensearch.action.admin.indices.datastream.DeleteDataStreamRequestTests.getClusterStateWithDataStreams;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GetDataStreamsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        final String[] searchParameter;
        switch (randomIntBetween(1, 4)) {
            case 1:
                searchParameter = generateRandomStringArray(3, 8, false, false);
                break;
            case 2:
                String[] parameters = generateRandomStringArray(3, 8, false, false);
                for (int k = 0; k < parameters.length; k++) {
                    parameters[k] = parameters[k] + "*";
                }
                searchParameter = parameters;
                break;
            case 3:
                searchParameter = new String[] { "*" };
                break;
            default:
                searchParameter = null;
                break;
        }
        return new Request(searchParameter);
    }

    public void testGetDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 1)), List.of());
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamName });
        List<DataStream> dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamName));
    }

    public void testGetDataStreamsWithWildcards() {
        final String[] dataStreamNames = { "my-data-stream", "another-data-stream" };
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamNames[0], 1), new Tuple<>(dataStreamNames[1], 1)),
            List.of()
        );

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamNames[1].substring(0, 5) + "*" });
        List<DataStream> dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));

        req = new GetDataStreamAction.Request(new String[] { "*" });
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request((String[]) null);
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request(new String[] { "matches-none*" });
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(0));
    }

    public void testGetDataStreamsWithoutWildcards() {
        final String[] dataStreamNames = { "my-data-stream", "another-data-stream" };
        ClusterState cs = getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamNames[0], 1), new Tuple<>(dataStreamNames[1], 1)),
            List.of()
        );

        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamNames[0], dataStreamNames[1] });
        List<DataStream> dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(2));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));
        assertThat(dataStreams.get(1).getName(), equalTo(dataStreamNames[0]));

        req = new GetDataStreamAction.Request(new String[] { dataStreamNames[1] });
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[1]));

        req = new GetDataStreamAction.Request(new String[] { dataStreamNames[0] });
        dataStreams = GetDataStreamAction.TransportAction.getDataStreams(
            cs,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            req
        );
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(dataStreams.get(0).getName(), equalTo(dataStreamNames[0]));

        GetDataStreamAction.Request req2 = new GetDataStreamAction.Request(new String[] { "foo" });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> GetDataStreamAction.TransportAction.getDataStreams(
                cs,
                new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
                req2
            )
        );
        assertThat(e.getMessage(), containsString("no such index [foo]"));
    }

    public void testGetNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        GetDataStreamAction.Request req = new GetDataStreamAction.Request(new String[] { dataStreamName });
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> GetDataStreamAction.TransportAction.getDataStreams(
                cs,
                new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
                req
            )
        );
        assertThat(e.getMessage(), containsString("no such index [" + dataStreamName + "]"));
    }

}
