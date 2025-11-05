package org.opensearch.action.admin.indices.mapping.get;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for TransportGetMappingsAction circuit breaker + estimation logic.
 */
public class TransportGetMappingsActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private ActionFilters actionFilters;
    private IndicesService indicesService;
    private CircuitBreakerService circuitBreakerService;
    private CircuitBreaker circuitBreaker;
    private IndexNameExpressionResolver resolver;
    private TransportGetMappingsAction action;

    @Captor
    private ArgumentCaptor<GetMappingsResponse> responseCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);
        actionFilters = mock(ActionFilters.class);
        indicesService = mock(IndicesService.class);
        circuitBreakerService = mock(CircuitBreakerService.class);
        circuitBreaker = mock(CircuitBreaker.class);
        resolver = mock(IndexNameExpressionResolver.class);

        when(circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(circuitBreaker);
        // Allow all fields for filtering during findMappings()
        when(indicesService.getFieldFilter()).thenReturn(index -> field -> true);

        action = new TransportGetMappingsAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            resolver,
            indicesService,
            circuitBreakerService
        );
    }

    public void testChargesAndReleasesForTwoIndices() throws IOException {
        // Given two indices with mappings whose compressed sizes we control
        final String[] indices = new String[] { "i1", "i2" };

        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata imd1 = mock(IndexMetadata.class);
        IndexMetadata imd2 = mock(IndexMetadata.class);
        MappingMetadata mm1 = mock(MappingMetadata.class);
        MappingMetadata mm2 = mock(MappingMetadata.class);
        final CompressedXContent src1 = new CompressedXContent("{\"properties\":{\"f1\":{\"type\":\"keyword\"}}}");
        final CompressedXContent src2 = new CompressedXContent("{\"properties\":{\"f2\":{\"type\":\"text\"}}}");

        // expected estimate is based on the actual compressed bytes
        final long expectedEstimate = RamUsageEstimator.sizeOf(src1.compressed()) + RamUsageEstimator.sizeOf(src2.compressed());

        when(imd1.mapping()).thenReturn(mm1);
        when(imd2.mapping()).thenReturn(mm2);
        when(mm1.source()).thenReturn(src1);
        when(mm2.source()).thenReturn(src2);

        when(state.metadata()).thenReturn(metadata);
        when(metadata.index("i1")).thenReturn(imd1);
        when(metadata.index("i2")).thenReturn(imd2);
        when(imd1.mapping()).thenReturn(mm1);
        when(imd2.mapping()).thenReturn(mm2);

        // The transport action asks metadata().findMappings(...) to build the response body.
        Map<String, MappingMetadata> responseMap = Map.of("i1", mm1, "i2", mm2);
        when(metadata.findMappings(eq(indices), any())).thenReturn(responseMap);

        @SuppressWarnings("unchecked")
        ActionListener<GetMappingsResponse> listener = mock(ActionListener.class);

        action.doClusterManagerOperation(new GetMappingsRequest(), indices, state, listener);

        // Then: charged with the sum estimate and released afterward
        verify(circuitBreaker, times(1)).addEstimateBytesAndMaybeBreak(eq(expectedEstimate), eq("get_mappings"));
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq(-expectedEstimate));

        // And the listener received the response carrying the same map
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        GetMappingsResponse resp = responseCaptor.getValue();

        assertEquals(responseMap, resp.mappings());
        verify(listener, never()).onFailure(any());
    }

    public void testNoReleaseWhenEstimateIsZero() throws IOException {
        // One index with null mapping → estimate should be 0; still "charged" call happens with 0
        final String[] indices = new String[] { "i0" };

        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata imd0 = mock(IndexMetadata.class);

        when(state.metadata()).thenReturn(metadata);
        when(metadata.index("i0")).thenReturn(imd0);
        when(imd0.mapping()).thenReturn(null); // no mapping -> contributes 0 bytes

        when(metadata.findMappings(eq(indices), any())).thenReturn(Map.of());

        @SuppressWarnings("unchecked")
        ActionListener<GetMappingsResponse> listener = mock(ActionListener.class);

        action.doClusterManagerOperation(new GetMappingsRequest(), indices, state, listener);

        // Called with 0
        verify(circuitBreaker, times(1)).addEstimateBytesAndMaybeBreak(eq(0L), eq("get_mappings"));
        // No release when estimate == 0 (guard in finally)
        verify(circuitBreaker, never()).addWithoutBreaking(anyLong());

        verify(listener, times(1)).onResponse(any());
        verify(listener, never()).onFailure(any());
    }

    public void testReleaseHappensWhenFindMappingsThrows() throws IOException {
        // If an exception occurs after the breaker is charged, we still release in finally
        final String[] indices = new String[] { "i1" };

        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata imd1 = mock(IndexMetadata.class);
        MappingMetadata mm1 = mock(MappingMetadata.class);
        final CompressedXContent src = new CompressedXContent("{\"properties\":{\"f\":{\"type\":\"integer\"}}}");
        final long expectedEstimate = RamUsageEstimator.sizeOf(src.compressed());

        when(imd1.mapping()).thenReturn(mm1);
        when(mm1.source()).thenReturn(src);

        // Simulate failure after breaker charge
        when(metadata.findMappings(eq(indices), any())).thenThrow(new RuntimeException("boom"));

        when(state.metadata()).thenReturn(metadata);
        when(metadata.index("i1")).thenReturn(imd1);
        when(imd1.mapping()).thenReturn(mm1);

        when(metadata.findMappings(eq(indices), any())).thenThrow(new RuntimeException("boom"));

        @SuppressWarnings("unchecked")
        ActionListener<GetMappingsResponse> listener = mock(ActionListener.class);

        action.doClusterManagerOperation(new GetMappingsRequest(), indices, state, listener);

        verify(circuitBreaker, times(1)).addEstimateBytesAndMaybeBreak(eq(expectedEstimate), eq("get_mappings"));
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq(-expectedEstimate));
        verify(listener, times(1)).onFailure(any(RuntimeException.class));
        verify(listener, never()).onResponse(any());
    }
}
