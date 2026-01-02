/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.create;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.indexmetadatacoordinator.TransportIndexMetadataCoordinatorAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.mapper.MappingTransformerRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Transport action for creating an index
 */
public class TransportCreateIndexAction extends TransportIndexMetadataCoordinatorAction<CreateIndexRequest, CreateIndexResponse> implements
    TransportIndicesResolvingAction<CreateIndexRequest> {

    private final MetadataCreateIndexService createIndexService;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    @Inject
    public TransportCreateIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            CreateIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateIndexRequest::new,
            indexNameExpressionResolver
        );
        this.createIndexService = createIndexService;
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateIndexResponse read(StreamInput in) throws IOException {
        return new CreateIndexResponse(in);
    }

    @Override
    protected void indexMetadataCoordinatorOperation(
        CreateIndexRequest request,
        ClusterState state,
        ActionListener<CreateIndexResponse> listener
    ) {

        assert state.nodes().getIndexMetadataCoordinatorNodeId() != null
            : "Index Metadata Coordinator node is not assigned yet";

        String imcNodeID = state.nodes().getIndexMetadataCoordinatorNodeId();
        String localNodeID = clusterService.localNode().getId();

        assert imcNodeID.equals(localNodeID)
            : "Current node is not Index Metadata Coordinator Node";

        logger.info("Received request to create index via Index Metadata Coordinator");

        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final String indexName = resolveIndexName(request);

        final String finalCause = cause;
        final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMappings -> {
            final CreateIndexIndexMetadataCoordinatorRequest updateRequest = (CreateIndexIndexMetadataCoordinatorRequest) new CreateIndexIndexMetadataCoordinatorRequest(
                finalCause,
                indexName,
                request.index()
            ).ackTimeout(request.timeout())
                .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
                .settings(request.settings())
                .mappings(transformedMappings)
                .aliases(request.aliases())
                .context(request.context())
                .waitForActiveShards(request.waitForActiveShards());

            createIndexService.createIndexViaIMC(
                updateRequest,
                ActionListener.map(
                    listener,
                    response -> new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName)
                )
            );
        }, listener::onFailure);

        mappingTransformerRegistry.applyTransformers(request.mappings(), null, mappingTransformListener);
    }

    @Override
    public ResolvedIndices resolveIndices(CreateIndexRequest request) {
        ResolvedIndices result = ResolvedIndices.of(resolveIndexName(request));

        if (request.aliases().isEmpty()) {
            return result;
        } else {
            return result.withLocalSubActions(
                IndicesAliasesAction.INSTANCE,
                ResolvedIndices.Local.of(request.aliases().stream().map(Alias::name).collect(Collectors.toSet()))
            );
        }
    }

    private String resolveIndexName(CreateIndexRequest request) {
        return indexNameExpressionResolver.resolveDateMathExpression(request.index());
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        ClusterBlockException clusterBlockException = state.blocks()
            .indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());

        if (clusterBlockException == null) {
            return state.blocks().createIndexBlockedException(ClusterBlockLevel.CREATE_INDEX);
        }
        return clusterBlockException;
    }
}
