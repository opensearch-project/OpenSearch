/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc.services.nodesInfo;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.node.info.proto.NodesInfoProto;
import org.opensearch.action.admin.cluster.node.info.proto.NodesInfoProtoService;
import org.opensearch.action.admin.cluster.node.info.proto.NodesInfoServiceGrpc;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.core.service.ReportingService;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.grpc.GrpcInfo;
import org.opensearch.http.HttpInfo;
import org.opensearch.ingest.IngestInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.search.aggregations.support.AggregationInfo;
import org.opensearch.search.pipeline.SearchPipelineInfo;
import org.opensearch.threadpool.ThreadPoolInfo;
import org.opensearch.transport.TransportInfo;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class NodesInfoServiceImpl extends NodesInfoServiceGrpc.NodesInfoServiceImplBase {
    private static final Logger logger = LogManager.getLogger(NodesInfoServiceImpl.class);
    private final NodeClient client;

    public NodesInfoServiceImpl(NodeClient client) {
        this.client = client;
    }

    @Override
    public void getNodesInfo(NodesInfoProtoService.NodesInfoRequestProto request, StreamObserver<NodesInfoProtoService.NodesInfoResponseProto> responseObserver) {
        NodesInfoResponse response = client.admin().cluster()
            .nodesInfo(reqFromProto(request))
            .actionGet();
        responseObserver.onNext(respToProto(response));
        responseObserver.onCompleted();
    }
    
    private static NodesInfoRequest reqFromProto(NodesInfoProtoService.NodesInfoRequestProto request) {
        String[] nodeIds = request.getNodeIdsList().toArray(new String[0]);
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(nodeIds);
        nodesInfoRequest.timeout(request.getTimeout());

        if (request.getMetrics().getAll()) {
            nodesInfoRequest.all();
        } else {
            for (String met : request.getMetrics().getMetricList().getStrListList()) {
                nodesInfoRequest.addMetric(met);
            }
        }

        return nodesInfoRequest;
    }

    private static NodesInfoProtoService.NodesInfoResponseProto respToProto(NodesInfoResponse response) {
        NodesInfoProtoService.NodesInfoResponseProto.Builder builder = NodesInfoProtoService.NodesInfoResponseProto.newBuilder();

        for (NodeInfo ni : response.getNodes()) {
            NodesInfoProto.NodesInfo.Builder nib = NodesInfoProto.NodesInfo.newBuilder()
                .setNodeId(ni.getNode().getId())
                .setNodeName(ni.getNode().getName())
                .setTransport(ni.getNode().getAddress().toString())
                .setHost(ni.getNode().getHostName())
                .setIp(ni.getNode().getHostAddress())
                .setIp(ni.getVersion().toString())
                .setBuildType(ni.getBuild().type().displayName())
                .setBuildType(ni.getBuild().hash());

            if (ni.getTotalIndexingBuffer() != null) {
                nib.setTotalIndexingBuffer(ni.getTotalIndexingBuffer().toString());
            }

            for (DiscoveryNodeRole role : ni.getNode().getRoles()) {
                nib.addRoles(role.roleName());
            }

            for (Map.Entry<String, String> entry : ni.getNode().getAttributes().entrySet()) {
                nib.putAttributes(entry.getKey(), entry.getValue());
            }

            try {
                XContentBuilder settingsBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
                String settingsXCont = ni.getSettings().toXContent(settingsBuilder, EMPTY_PARAMS).toString();
                nib.setSettings(settingsXCont);
            } catch (IOException e) {
                logger.debug("Failed to build NodesInfo settings: " + e);
            }

            Optional.ofNullable(getNodeInfoJSON(ni, OsInfo.class)).ifPresent(nib::setOs);
            Optional.ofNullable(getNodeInfoJSON(ni, ProcessInfo.class)).ifPresent(nib::setProcess);
            Optional.ofNullable(getNodeInfoJSON(ni, JvmInfo.class)).ifPresent(nib::setJvm);
            Optional.ofNullable(getNodeInfoJSON(ni, ThreadPoolInfo.class)).ifPresent(nib::setThreadPool);
            Optional.ofNullable(getNodeInfoJSON(ni, TransportInfo.class)).ifPresent(nib::setTransport);
            Optional.ofNullable(getNodeInfoJSON(ni, HttpInfo.class)).ifPresent(nib::setHttp);
            Optional.ofNullable(getNodeInfoJSON(ni, GrpcInfo.class)).ifPresent(nib::setGrpc);
            Optional.ofNullable(getNodeInfoJSON(ni, PluginsAndModules.class)).ifPresent(nib::setPlugins);
            Optional.ofNullable(getNodeInfoJSON(ni, IngestInfo.class)).ifPresent(nib::setIngest);
            Optional.ofNullable(getNodeInfoJSON(ni, AggregationInfo.class)).ifPresent(nib::setAggs);
            Optional.ofNullable(getNodeInfoJSON(ni, SearchPipelineInfo.class)).ifPresent(nib::setSearchPipelines);

            builder.addNodesInfo(nib);
        }

        return builder.build();
    }

    private static <T extends ReportingService.Info> String getNodeInfoJSON(NodeInfo n, Class<T> clazz) {
        try {
            XContentBuilder settingsBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
            return n.getInfo(OsInfo.class).toXContent(settingsBuilder, EMPTY_PARAMS).toString();
        } catch (IOException e) {
            logger.debug("Failed to build NodesInfo os: " + e);
        }
        return null;
    }
}
