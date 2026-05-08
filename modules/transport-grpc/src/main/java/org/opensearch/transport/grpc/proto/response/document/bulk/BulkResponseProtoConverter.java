/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.bulk;

import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.Item;
import org.opensearch.protobufs.ResponseItem;

/**
 * Converts a {@code protobufs.BulkResponse} (received over gRPC) into the server-internal {@link BulkResponse}.
 *
 * <p>This is the inverse of {@link BulkResponseProtoUtils} which lives next door. Co-locating both directions enables a
 * round-trip parity test that catches divergence as the schema evolves.
 *
 * <p><b>Known lossy conversions</b> (limitations of the proto schema, not bugs in this converter):
 * <ul>
 *   <li>Proto carries no shard UUID or shard id, so each item is given a placeholder
 *       {@link ShardId} ({@link Index} uuid {@code "_na_"}, shard id {@code -1}).</li>
 *   <li>The wire-level {@code status} (a REST status int) is many-to-one with {@link DocWriteResponse.Result}; we derive
 *       the {@code Result} enum from the {@code result} string field and let the response object compute its own status.</li>
 *   <li>Failure subclasses are unrecoverable from the proto representation: every failure becomes a
 *       {@link BulkItemResponse.Failure} backed by {@link OpenSearchException} regardless of the original server-side
 *       exception type.</li>
 * </ul>
 */
public final class BulkResponseProtoConverter {

    private BulkResponseProtoConverter() {}

    /**
     * Converts a {@code protobufs.BulkResponse} into a server-internal {@link BulkResponse}.
     *
     * @param proto the proto response received over gRPC
     * @return the equivalent {@link BulkResponse}
     */
    public static BulkResponse fromProto(org.opensearch.protobufs.BulkResponse proto) {
        int n = proto.getItemsCount();
        BulkItemResponse[] items = new BulkItemResponse[n];
        for (int i = 0; i < n; i++) {
            items[i] = fromProto(i, proto.getItems(i));
        }
        long ingestTook = proto.hasIngestTook() ? proto.getIngestTook() : -1L;
        return new BulkResponse(items, proto.getTook(), ingestTook);
    }

    private static BulkItemResponse fromProto(int id, Item protoItem) {
        switch (protoItem.getItemCase()) {
            case INDEX:
                return buildItem(id, DocWriteRequest.OpType.INDEX, protoItem.getIndex());
            case CREATE:
                return buildItem(id, DocWriteRequest.OpType.CREATE, protoItem.getCreate());
            case UPDATE:
                return buildItem(id, DocWriteRequest.OpType.UPDATE, protoItem.getUpdate());
            case DELETE:
                return buildItem(id, DocWriteRequest.OpType.DELETE, protoItem.getDelete());
            case ITEM_NOT_SET:
            default:
                throw new IllegalStateException("Item has no operation case set");
        }
    }

    private static BulkItemResponse buildItem(int id, DocWriteRequest.OpType opType, ResponseItem ri) {
        if (ri.hasError()) {
            return new BulkItemResponse(id, opType, toFailure(ri));
        }

        DocWriteResponse response;
        switch (opType) {
            case INDEX:
            case CREATE: {
                IndexResponse ir = new IndexResponse(
                    shardIdFor(ri),
                    ri.hasXId() ? ri.getXId() : null,
                    ri.hasXSeqNo() ? ri.getXSeqNo() : -2L,
                    ri.hasXPrimaryTerm() ? ri.getXPrimaryTerm() : 0L,
                    ri.hasXVersion() ? ri.getXVersion() : -1L,
                    parseResult(ri.getResult()) == DocWriteResponse.Result.CREATED
                );
                ir.setShardInfo(toShardInfo(ri));
                ir.setForcedRefresh(ri.getForcedRefresh());
                response = ir;
                break;
            }
            case DELETE: {
                DeleteResponse dr = new DeleteResponse(
                    shardIdFor(ri),
                    ri.hasXId() ? ri.getXId() : null,
                    ri.hasXSeqNo() ? ri.getXSeqNo() : -2L,
                    ri.hasXPrimaryTerm() ? ri.getXPrimaryTerm() : 0L,
                    ri.hasXVersion() ? ri.getXVersion() : -1L,
                    parseResult(ri.getResult()) != DocWriteResponse.Result.NOT_FOUND
                );
                dr.setShardInfo(toShardInfo(ri));
                dr.setForcedRefresh(ri.getForcedRefresh());
                response = dr;
                break;
            }
            case UPDATE: {
                UpdateResponse ur = new UpdateResponse(
                    shardIdFor(ri),
                    ri.hasXId() ? ri.getXId() : null,
                    ri.hasXSeqNo() ? ri.getXSeqNo() : -2L,
                    ri.hasXPrimaryTerm() ? ri.getXPrimaryTerm() : 0L,
                    ri.hasXVersion() ? ri.getXVersion() : -1L,
                    parseResult(ri.getResult())
                );
                ur.setShardInfo(toShardInfo(ri));
                ur.setForcedRefresh(ri.getForcedRefresh());
                response = ur;
                break;
            }
            default:
                throw new IllegalStateException("unsupported op type: " + opType);
        }
        return new BulkItemResponse(id, opType, response);
    }

    private static BulkItemResponse.Failure toFailure(ResponseItem ri) {
        org.opensearch.protobufs.ErrorCause err = ri.getError();
        String reason = err.hasReason() ? err.getReason() : err.getType();
        Exception cause = new OpenSearchException("[" + err.getType() + "] " + reason);
        return new BulkItemResponse.Failure(ri.getXIndex(), ri.hasXId() ? ri.getXId() : null, cause);
    }

    private static ShardId shardIdFor(ResponseItem ri) {
        return new ShardId(new Index(ri.getXIndex(), "_na_"), -1);
    }

    private static ReplicationResponse.ShardInfo toShardInfo(ResponseItem ri) {
        if (ri.hasXShards() == false) {
            return new ReplicationResponse.ShardInfo();
        }
        org.opensearch.protobufs.ShardInfo s = ri.getXShards();
        return new ReplicationResponse.ShardInfo(s.getTotal(), s.getSuccessful());
    }

    private static DocWriteResponse.Result parseResult(String result) {
        if (result == null || result.isEmpty()) {
            return DocWriteResponse.Result.NOOP;
        }
        switch (result.toLowerCase(java.util.Locale.ROOT)) {
            case "created":
                return DocWriteResponse.Result.CREATED;
            case "updated":
                return DocWriteResponse.Result.UPDATED;
            case "deleted":
                return DocWriteResponse.Result.DELETED;
            case "not_found":
                return DocWriteResponse.Result.NOT_FOUND;
            case "noop":
                return DocWriteResponse.Result.NOOP;
            default:
                throw new IllegalArgumentException("unknown result: " + result);
        }
    }
}
