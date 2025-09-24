/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.protobufs.IdsQuery;

/**
 * Utility class for converting IdsQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of ids queries
 * into their corresponding OpenSearch IdsQueryBuilder implementations for search operations.
 */
public class IdsQueryBuilderProtoUtils {

    private IdsQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer IdsQuery to an OpenSearch IdsQueryBuilder.
     * Similar to {@link IdsQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * IdsQueryBuilder with the appropriate ids, boost, and query name.
     *
     * @param idsQueryProto The Protocol Buffer IdsQuery object
     * @return A configured IdsQueryBuilder instance
     */
    public static IdsQueryBuilder fromProto(IdsQuery idsQueryProto) {
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        // Create IdsQueryBuilder
        IdsQueryBuilder idsQuery = new IdsQueryBuilder();

        // Process name
        if (idsQueryProto.hasXName()) {
            queryName = idsQueryProto.getXName();
            idsQuery.queryName(queryName);
        }

        // Process boost
        if (idsQueryProto.hasBoost()) {
            boost = idsQueryProto.getBoost();
            idsQuery.boost(boost);
        }

        // Process values (ids)
        if (idsQueryProto.getValuesCount() > 0) {
            String[] ids = new String[idsQueryProto.getValuesCount()];
            for (int i = 0; i < idsQueryProto.getValuesCount(); i++) {
                ids[i] = idsQueryProto.getValues(i);
            }
            idsQuery.addIds(ids);
        }

        return idsQuery;
    }
}
