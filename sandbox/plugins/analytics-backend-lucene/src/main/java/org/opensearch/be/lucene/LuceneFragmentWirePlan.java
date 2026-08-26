/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.List;

/** Typed wire payload shared by Lucene fragment conversion and shard execution. */
record LuceneFragmentWirePlan(List<String> outputNames, byte[] filterBytes, ArrowBatchSourcePlan arrowSourcePlan) {

    LuceneFragmentWirePlan {
        outputNames = List.copyOf(outputNames);
        filterBytes = filterBytes == null ? null : filterBytes.clone();
    }

    @Override
    public byte[] filterBytes() {
        return filterBytes == null ? null : filterBytes.clone();
    }

    static LuceneFragmentWirePlan create(List<String> outputNames, QueryBuilder filter, ArrowBatchSourcePlan arrowSourcePlan) {
        return new LuceneFragmentWirePlan(outputNames, serializeFilter(filter), arrowSourcePlan);
    }

    static LuceneFragmentWirePlan fromBytes(byte[] bytes) {
        try (StreamInput input = StreamInput.wrap(bytes)) {
            LuceneFragmentWirePlan plan = new LuceneFragmentWirePlan(
                input.readStringList(),
                readOptionalBytes(input),
                input.readOptionalWriteable(ArrowBatchSourcePlan::new)
            );
            if (input.available() != 0) {
                throw new IllegalStateException("Unexpected trailing Lucene fragment bytes");
            }
            return plan;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize Lucene fragment", e);
        }
    }

    LuceneFragmentWirePlan withOutputNames(List<String> names) {
        return new LuceneFragmentWirePlan(names, filterBytes, arrowSourcePlan);
    }

    LuceneFragmentWirePlan withArrowSourcePlan(ArrowBatchSourcePlan plan, List<String> names) {
        return new LuceneFragmentWirePlan(names, filterBytes, plan);
    }

    QueryBuilder filterQuery(NamedWriteableRegistry registry) {
        if (filterBytes == null) {
            return null;
        }
        try (StreamInput rawInput = StreamInput.wrap(filterBytes)) {
            StreamInput input = new NamedWriteableAwareStreamInput(rawInput, registry);
            QueryBuilder filter = input.readNamedWriteable(QueryBuilder.class);
            if (input.available() != 0) {
                throw new IllegalStateException("Unexpected trailing Lucene filter bytes");
            }
            return filter;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize Lucene filter", e);
        }
    }

    byte[] toBytes() {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeStringCollection(outputNames);
            output.writeBoolean(filterBytes != null);
            if (filterBytes != null) {
                output.writeByteArray(filterBytes);
            }
            output.writeOptionalWriteable(arrowSourcePlan);
            return BytesReference.toBytes(output.bytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene fragment", e);
        }
    }

    private static byte[] readOptionalBytes(StreamInput input) throws IOException {
        return input.readBoolean() ? input.readByteArray() : null;
    }

    private static byte[] serializeFilter(QueryBuilder filter) {
        if (filter == null) {
            return null;
        }
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(filter);
            return BytesReference.toBytes(output.bytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene filter", e);
        }
    }
}
