/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import static org.opensearch.test.OpenSearchIntegTestCase.Scope;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class DataStreamTestCase extends OpenSearchIntegTestCase {

    public AcknowledgedResponse createDataStream(String name) throws Exception {
        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(name);
        AcknowledgedResponse response = client().admin().indices().createDataStream(request).get();
        assertThat(response.isAcknowledged(), is(true));
        performRemoteStoreTestAction();
        return response;
    }

    public AcknowledgedResponse deleteDataStreams(String... names) throws Exception {
        DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(names);
        AcknowledgedResponse response = client().admin().indices().deleteDataStream(request).get();
        assertThat(response.isAcknowledged(), is(true));
        return response;
    }

    public GetDataStreamAction.Response getDataStreams(String... names) throws Exception {
        GetDataStreamAction.Request request = new GetDataStreamAction.Request(names);
        return client().admin().indices().getDataStreams(request).get();
    }

    public List<String> getDataStreamsNames(String... names) throws Exception {
        return getDataStreams(names).getDataStreams().stream().map(dsInfo -> dsInfo.getDataStream().getName()).collect(Collectors.toList());
    }

    public DataStreamsStatsAction.Response getDataStreamsStats(String... names) throws Exception {
        DataStreamsStatsAction.Request request = new DataStreamsStatsAction.Request();
        request.indices(names);
        return client().execute(DataStreamsStatsAction.INSTANCE, request).get();
    }

    public RolloverResponse rolloverDataStream(String name) throws Exception {
        RolloverRequest request = new RolloverRequest(name, null);
        RolloverResponse response = client().admin().indices().rolloverIndex(request).get();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(response.isRolledOver(), is(true));
        performRemoteStoreTestAction();
        return response;
    }

    public AcknowledgedResponse createDataStreamIndexTemplate(String name, List<String> indexPatterns) throws Exception {
        return createDataStreamIndexTemplate(name, indexPatterns, "@timestamp");
    }

    public AcknowledgedResponse createDataStreamIndexTemplate(String name, List<String> indexPatterns, String timestampFieldName)
        throws Exception {
        ComposableIndexTemplate template = new ComposableIndexTemplate(
            indexPatterns,
            new Template(Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 1).build(), null, null),
            null,
            null,
            null,
            null,
            new ComposableIndexTemplate.DataStreamTemplate(new DataStream.TimestampField(timestampFieldName))
        );

        return createIndexTemplate(name, template);
    }

    public AcknowledgedResponse createIndexTemplate(String name, String jsonContent) throws Exception {
        XContentParser parser = XContentHelper.createParser(xContentRegistry(), null, new BytesArray(jsonContent), MediaTypeRegistry.JSON);

        return createIndexTemplate(name, ComposableIndexTemplate.parse(parser));
    }

    private AcknowledgedResponse createIndexTemplate(String name, ComposableIndexTemplate template) throws Exception {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(name);
        request.indexTemplate(template);
        AcknowledgedResponse response = client().execute(PutComposableIndexTemplateAction.INSTANCE, request).get();
        assertThat(response.isAcknowledged(), is(true));
        return response;
    }

    public AcknowledgedResponse deleteIndexTemplate(String name) throws Exception {
        DeleteComposableIndexTemplateAction.Request request = new DeleteComposableIndexTemplateAction.Request(name);
        AcknowledgedResponse response = client().execute(DeleteComposableIndexTemplateAction.INSTANCE, request).get();
        assertThat(response.isAcknowledged(), is(true));
        return response;
    }
}
