/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.TranslogLeafReader;
import org.opensearch.index.fieldvisitor.FieldsVisitor;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class for comparing two index operations with the support of derived source, for which we will have to first
 * regenerate the source and perform the deep equal of both sources.
 *
 * @opensearch.internal
 */
@PublicApi(since = "3.1.0")
public abstract class TranslogOperationHelper {

    private static final Logger logger = LogManager.getLogger(TranslogOperationHelper.class);

    public static final TranslogOperationHelper DEFAULT = new TranslogOperationHelper() {
    };

    private TranslogOperationHelper() {}

    public static TranslogOperationHelper create(final EngineConfig engineConfig) {
        return new TranslogOperationHelper() {

            @Override
            public boolean hasSameIndexOperation(Translog.Index op1, Translog.Index op2) {
                boolean hasSameOp = Objects.equals(op1.id(), op2.id())
                    && Objects.equals(op1.routing(), op2.routing())
                    && op1.primaryTerm() == op2.primaryTerm()
                    && op1.seqNo() == op2.seqNo()
                    && op1.version() == op2.version();
                if (engineConfig.getIndexSettings().isDerivedSourceEnabled()) {
                    hasSameOp &= (compareSourcesWithOrder(deriveSource(op1, engineConfig), op2.source())
                        || compareSourcesWithOrder(op1.source(), deriveSource(op2, engineConfig)));
                } else {
                    hasSameOp &= Objects.equals(op1.source(), op2.source());
                }
                return hasSameOp;
            }
        };
    }

    public boolean hasSameIndexOperation(Translog.Index op1, Translog.Index op2) {
        return Objects.equals(op1.id(), op2.id())
            && Objects.equals(op1.source(), op2.source())
            && Objects.equals(op1.routing(), op2.routing())
            && op1.primaryTerm() == op2.primaryTerm()
            && op1.seqNo() == op2.seqNo()
            && op1.version() == op2.version();
    }

    private static BytesReference deriveSource(Translog.Index op, EngineConfig engineConfig) {
        try (TranslogLeafReader leafReader = new TranslogLeafReader(op, engineConfig)) {
            final FieldsVisitor visitor = new FieldsVisitor(true);
            leafReader.storedFields().document(0, visitor); // As reader will have only a single document, segment level doc id will be 0
            return visitor.source();
        } catch (IOException e) {
            return null;
        }
    }

    public static boolean compareSourcesWithOrder(BytesReference source1, BytesReference source2) {
        if (source1 == null && source2 == null) {
            return true;
        }
        if (source1 == null || source2 == null) {
            return false;
        }
        // Convert both sources to ordered maps
        try {
            Map<String, Object> map1 = XContentHelper.convertToMap(source1, true).v2();
            Map<String, Object> map2 = XContentHelper.convertToMap(source2, true).v2();

            return Objects.deepEquals(map1, map2);
        } catch (Exception e) {
            // Invalid source
            logger.error("Failed to convert source to map", e);
            return false;
        }
    }
}
