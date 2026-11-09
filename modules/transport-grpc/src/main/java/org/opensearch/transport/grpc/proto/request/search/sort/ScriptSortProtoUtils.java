/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.ScriptSort;
import org.opensearch.script.Script;
import org.opensearch.search.sort.ScriptSortBuilder;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for converting ScriptSort Protocol Buffers to OpenSearch ScriptSortBuilder objects.
 * Similar to {@link ScriptSortBuilder#fromXContent}, this class handles the conversion of
 * Protocol Buffer representations to properly configured ScriptSortBuilder objects with
 * script-based sorting, script type, sort mode, and nested sorting settings.
 *
 * @opensearch.internal
 */
public class ScriptSortProtoUtils {

    private ScriptSortProtoUtils() {
        // Utility class
    }

    /**
     * Converts a Protocol Buffer ScriptSort to a ScriptSortBuilder.
     * Similar to {@link ScriptSortBuilder#fromXContent}, this method parses the
     * Protocol Buffer representation and creates a properly configured ScriptSortBuilder
     * with the appropriate script, type, sort order, mode, and nested sorting settings.
     *
     * @param scriptSort The Protocol Buffer ScriptSort to convert
     * @param registry The registry for query conversion (needed for nested sorts with filters)
     * @return A configured ScriptSortBuilder
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public static ScriptSortBuilder fromProto(ScriptSort scriptSort, QueryBuilderProtoConverterRegistry registry) {
        if (scriptSort == null) {
            throw new IllegalArgumentException("ScriptSort cannot be null");
        }

        if (!scriptSort.hasScript()) {
            throw new IllegalArgumentException("ScriptSort must have a script");
        }

        if (!scriptSort.hasType()) {
            throw new IllegalArgumentException("ScriptSort must have a type");
        }

        Script script = ScriptProtoUtils.parseFromProtoRequest(scriptSort.getScript());

        if (scriptSort.getType() == org.opensearch.protobufs.ScriptSortType.SCRIPT_SORT_TYPE_UNSPECIFIED) {
            throw new IllegalArgumentException("ScriptSortType must be specified");
        }
        ScriptSortBuilder.ScriptSortType type = ScriptSortBuilder.ScriptSortType.fromString(
            ProtobufEnumUtils.convertToString(scriptSort.getType())
        );

        ScriptSortBuilder builder = new ScriptSortBuilder(script, type);

        if (scriptSort.hasOrder()) {
            builder.order(SortOrder.fromString(ProtobufEnumUtils.convertToString(scriptSort.getOrder())));
        }

        if (scriptSort.hasMode()) {
            builder.sortMode(SortMode.fromString(ProtobufEnumUtils.convertToString(scriptSort.getMode())));
        }

        if (scriptSort.hasNested()) {
            builder.setNestedSort(NestedSortProtoUtils.fromProto(scriptSort.getNested(), registry));
        }

        return builder;
    }

}
