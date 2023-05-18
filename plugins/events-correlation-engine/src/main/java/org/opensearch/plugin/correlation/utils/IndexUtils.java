/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.utils;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.core.ParseField.CommonFields._META;

/**
 * Index Management utils
 *
 * @opensearch.internal
 */
public class IndexUtils {
    private static final Integer NO_SCHEMA_VERSION = 0;
    private static final String SCHEMA_VERSION = "schema_version";

    /**
     * manages the mappings lifecycle for correlation rule index
     */
    public static Boolean correlationRuleIndexUpdated = false;

    private IndexUtils() {}

    /**
     * updates the status of correlationRuleIndexUpdated to true
     */
    public static void correlationRuleIndexUpdated() {
        correlationRuleIndexUpdated = true;
    }

    /**
     * util method which decides based on schema version whether to update an index.
     * @param index IndexMetadata
     * @param mapping new mappings
     * @return Boolean
     * @throws IOException IOException
     */
    public static Boolean shouldUpdateIndex(IndexMetadata index, String mapping) throws IOException {
        Integer oldVersion = NO_SCHEMA_VERSION;
        Integer newVersion = getSchemaVersion(mapping);

        Map<String, Object> indexMapping = index.mapping().sourceAsMap();
        if (indexMapping != null
            && indexMapping.containsKey(_META.getPreferredName())
            && indexMapping.get(_META.getPreferredName()) instanceof HashMap<?, ?>) {
            Map<?, ?> metaData = (HashMap<?, ?>) indexMapping.get(_META.getPreferredName());
            if (metaData.containsKey(SCHEMA_VERSION)) {
                oldVersion = (Integer) metaData.get(SCHEMA_VERSION);
            }
        }
        return newVersion > oldVersion;
    }

    /**
     * Gets the schema version for the mapping
     * @param mapping mappings as input
     * @return schema version
     * @throws IOException IOException
     */
    public static Integer getSchemaVersion(String mapping) throws IOException {
        XContentParser xcp = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, mapping);

        while (!xcp.isClosed()) {
            XContentParser.Token token = xcp.currentToken();
            if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                if (!Objects.equals(xcp.currentName(), _META.getPreferredName())) {
                    xcp.nextToken();
                    xcp.skipChildren();
                } else {
                    while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                        switch (xcp.currentName()) {
                            case SCHEMA_VERSION:
                                int version = xcp.intValue();
                                if (version < 0) {
                                    throw new IllegalArgumentException(
                                        String.format(Locale.getDefault(), "%s cannot be negative", SCHEMA_VERSION)
                                    );
                                }
                                return version;
                            default:
                                xcp.nextToken();
                        }
                    }
                }
            }
            xcp.nextToken();
        }
        return NO_SCHEMA_VERSION;
    }

    /**
     * updates the mappings for the index.
     * @param index index for which mapping needs to be updated
     * @param mapping new mappings
     * @param clusterState ClusterState
     * @param client Admin client
     * @param actionListener listener
     * @throws IOException IOException
     */
    public static void updateIndexMapping(
        String index,
        String mapping,
        ClusterState clusterState,
        IndicesAdminClient client,
        ActionListener<AcknowledgedResponse> actionListener
    ) throws IOException {
        if (clusterState.metadata().indices().containsKey(index)) {
            if (shouldUpdateIndex(clusterState.metadata().index(index), mapping)) {
                PutMappingRequest putMappingRequest = new PutMappingRequest(index).source(mapping, XContentType.JSON);
                client.putMapping(putMappingRequest, actionListener);
            } else {
                actionListener.onResponse(new AcknowledgedResponse(true));
            }
        }
    }
}
