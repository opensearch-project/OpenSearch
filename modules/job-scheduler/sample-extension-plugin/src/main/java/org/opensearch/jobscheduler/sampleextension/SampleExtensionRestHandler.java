/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.sampleextension;

import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.action.ActionListener;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A sample rest handler that supports schedule and deschedule job operation
 *
 * Users need to provide "id", "index", "job_name", and "interval" parameter to schedule
 * a job. e.g.
 * {@code 
 * POST /_plugins/scheduler_sample/watch?id=dashboards-job-id&job_name=watch dashboards index&index=.opensearch_dashboards_1&interval=1
 * }
 *
 * creates a job with id "dashboards-job-id" and job name "watch dashboards index", 
 * which logs ".opensearch_dashboards_1" index's shards info every 1 minute
 *
 * Users can remove that job by calling
 * {@code DELETE /_plugins/scheduler_sample/watch?id=dashboards-job-id}
 */
public class SampleExtensionRestHandler extends BaseRestHandler {
    public static final String WATCH_INDEX_URI = "/_plugins/scheduler_sample/watch";

    @Override
    public String getName() {
        return "Sample JobScheduler extension handler";
    }

    @Override
    public List<Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(
                new Route(RestRequest.Method.POST, WATCH_INDEX_URI),
                new Route(RestRequest.Method.DELETE, WATCH_INDEX_URI)
        ));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method().equals(RestRequest.Method.POST)) {
            // compose SampleJobParameter object from request
            String id = request.param("id");
            String indexName = request.param("index");
            String jobName = request.param("job_name");
            String interval = request.param("interval");
            String lockDurationSecondsString = request.param("lock_duration_seconds");
            Long lockDurationSeconds = lockDurationSecondsString != null ? Long.parseLong(lockDurationSecondsString) : null;
            String jitterString = request.param("jitter");
            Double jitter = jitterString != null ? Double.parseDouble(jitterString) : null;

            if(id == null || indexName ==null) {
                throw new IllegalArgumentException("Must specify id and index parameter");
            }
            SampleJobParameter jobParameter = new SampleJobParameter(id, jobName, indexName,
                    new IntervalSchedule(Instant.now(), Integer.parseInt(interval), ChronoUnit.MINUTES), lockDurationSeconds, jitter);
            IndexRequest indexRequest = new IndexRequest()
                    .index(SampleExtensionPlugin.JOB_INDEX_NAME)
                    .id(id)
                    .source(jobParameter.toXContent(JsonXContent.contentBuilder(), null));

            return restChannel -> {
                // index the job parameter
                client.index(indexRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        try {
                            RestResponse restResponse = new BytesRestResponse(RestStatus.OK,
                                    indexResponse.toXContent(JsonXContent.contentBuilder(), null));
                            restChannel.sendResponse(restResponse);
                        } catch(IOException e) {
                            restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                    }
                });
            };
        } else if (request.method().equals(RestRequest.Method.DELETE)) {
            // delete job parameter doc from index
            String id = request.param("id");
            DeleteRequest deleteRequest = new DeleteRequest()
                    .index(SampleExtensionPlugin.JOB_INDEX_NAME)
                    .id(id);

            return restChannel -> {
                client.delete(deleteRequest, new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, "Job deleted."));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                    }
                });
            };
        } else {
            return restChannel -> {
                restChannel.sendResponse(new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, request.method() + " is not allowed."));
            };
        }
    }
}
