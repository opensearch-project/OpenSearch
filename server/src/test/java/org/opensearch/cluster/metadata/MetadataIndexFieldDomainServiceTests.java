/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.fielddomain.PutIndexFieldDomainsClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MetadataIndexFieldDomainServiceTests extends OpenSearchTestCase {
    private static final IndexFieldDomainMetadata FIELD_DOMAIN_METADATA = IndexFieldDomainMetadata.getInstance();

    public void testApplyFieldDomainsPublishesMetadata() {
        Map<String, String> existing = new HashMap<>();
        existing.put("fields.host.name.type", "term_set");
        existing.put("fields.host.name.value", "server-a");
        ClusterState state = clusterState(indexMetadata("logs-000001", "index-uuid", existing));

        ClusterState updated = MetadataIndexFieldDomainService.applyFieldDomains(
            state,
            request("logs-000001", "index-uuid", new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"))
        );

        Map<String, String> customData = updated.metadata().index("logs-000001").getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY);
        assertThat(customData.get("fields.@timestamp.type"), equalTo("date_range"));
        assertThat(customData.get("fields.@timestamp.min"), equalTo("100"));
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.host.name.type"), equalTo("term_set"));
        assertThat(customData.get("fields.host.name.value"), equalTo("server-a"));
    }

    public void testApplyFieldDomainsReturnsSameStateWhenMetadataIsUnchanged() {
        DateRangeFieldDomain domain = new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test");
        ClusterState state = clusterState(indexMetadata("logs-000001", "index-uuid", FIELD_DOMAIN_METADATA.toCustomData(domain)));

        ClusterState updated = MetadataIndexFieldDomainService.applyFieldDomains(state, request("logs-000001", "index-uuid", domain));

        assertSame(state, updated);
    }

    public void testApplyFieldDomainsFailsWhenIndexIsMissing() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();

        expectThrows(
            IndexNotFoundException.class,
            () -> MetadataIndexFieldDomainService.applyFieldDomains(
                state,
                request("logs-000001", "index-uuid", new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"))
            )
        );
    }

    public void testApplyFieldDomainsFailsOnIndexUUIDMismatch() {
        ClusterState state = clusterState(indexMetadata("logs-000001", "actual-uuid", Map.of()));

        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> MetadataIndexFieldDomainService.applyFieldDomains(
                state,
                request("logs-000001", "expected-uuid", new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"))
            )
        );

        assertThat(exception.getMessage(), containsString("no such index [logs-000001]"));
        assertNotNull(exception.getCause());
        assertThat(exception.getCause().getMessage(), containsString("expected: [expected-uuid]"));
        assertThat(exception.getCause().getMessage(), containsString("got: [actual-uuid]"));
    }

    public void testApplyFieldDomainsFailsOnMissingMetadata() {
        ClusterState state = clusterState(indexMetadata("logs-000001", "index-uuid", Map.of()));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataIndexFieldDomainService.applyFieldDomains(
                state,
                new PutIndexFieldDomainsClusterStateUpdateRequest().targetIndex(new Index("logs-000001", "index-uuid"))
            )
        );

        assertThat(exception.getMessage(), equalTo("field domain metadata is required"));
    }

    private static PutIndexFieldDomainsClusterStateUpdateRequest request(String index, String indexUUID, DateRangeFieldDomain domain) {
        return new PutIndexFieldDomainsClusterStateUpdateRequest().targetIndex(new Index(index, indexUUID))
            .fieldDomainCustomData(FIELD_DOMAIN_METADATA.toCustomData(domain));
    }

    private static ClusterState clusterState(IndexMetadata indexMetadata) {
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexMetadata, false)).build();
    }

    private static IndexMetadata indexMetadata(String index, String indexUUID, Map<String, String> customData) {
        IndexMetadata.Builder builder = IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0);
        if (customData.isEmpty() == false) {
            builder.putCustom(IndexFieldDomainMetadata.CUSTOM_KEY, customData);
        }
        return builder.build();
    }
}
