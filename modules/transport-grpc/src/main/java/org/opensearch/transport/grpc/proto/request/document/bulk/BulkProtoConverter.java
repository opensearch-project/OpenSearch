/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import com.google.protobuf.ByteString;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.VersionType;
import org.opensearch.index.seqno.SequenceNumbers;

/**
 * Converts a server-internal {@link BulkRequest} into a {@code protobufs.BulkRequest}.
 *
 * <p>This is the inverse of {@link BulkRequestProtoUtils#prepareRequest}, which lives next door and translates the same
 * proto type back into a server-internal {@code BulkRequest}. Co-locating both directions enables round-trip parity tests
 * that catch divergence as the schema evolves.
 *
 * <p>This converter exists so client libraries (HLRC's API model, opensearch-java in the future) can take a server-internal
 * request object and emit a proto for transmission over gRPC. The cluster-side code already in this module then consumes
 * that proto and reconstructs the server-internal object.
 *
 * <p><b>Canonicalization rules</b> (chosen so {@code prepareRequest(toProto(s))} reconstructs an equivalent
 * {@code BulkRequest}):
 * <ul>
 *   <li>{@link RefreshPolicy#NONE} maps to proto unset (default {@code REFRESH_UNSPECIFIED}); the forward path treats both
 *       {@code REFRESH_FALSE} and {@code REFRESH_UNSPECIFIED} as {@code RefreshPolicy.NONE}, so we pick the unset form.</li>
 *   <li>{@link VersionType#INTERNAL} (server default) maps to proto unset.</li>
 *   <li>{@link ActiveShardCount#DEFAULT} maps to proto unset.</li>
 *   <li>Unassigned {@code ifSeqNo} / {@code ifPrimaryTerm} are not emitted.</li>
 *   <li>{@code Versions.MATCH_ANY} is not emitted as a {@code version} field.</li>
 *   <li>Server {@code globalIndex} / {@code globalRouting} / {@code globalPipeline} / {@code globalRequireAlias} are not
 *       carried — the forward path inlines top-level proto defaults into per-row requests at parse time and never reads
 *       these slots back, so the round-trippable form is per-row only.</li>
 *   <li>A {@code CREATE} op type with version control is emitted as {@code IndexOperation} with {@code op_type=CREATE},
 *       since {@code WriteOperation} (the dedicated CREATE proto) lacks those fields in the current schema.</li>
 * </ul>
 */
public final class BulkProtoConverter {

    private BulkProtoConverter() {}

    /**
     * Converts a server-internal {@link BulkRequest} to its protobuf equivalent.
     *
     * @param serverBulk the request to convert
     * @return the equivalent {@code protobufs.BulkRequest}
     */
    public static org.opensearch.protobufs.BulkRequest toProto(BulkRequest serverBulk) {
        org.opensearch.protobufs.BulkRequest.Builder builder = org.opensearch.protobufs.BulkRequest.newBuilder();

        if (serverBulk.timeout() != null) {
            builder.setTimeout(serverBulk.timeout().getStringRep());
        }

        org.opensearch.protobufs.Refresh refresh = toProtoRefresh(serverBulk.getRefreshPolicy());
        if (refresh != null) {
            builder.setRefresh(refresh);
        }

        org.opensearch.protobufs.WaitForActiveShards was = toProtoWaitForActiveShards(serverBulk.waitForActiveShards());
        if (was != null) {
            builder.setWaitForActiveShards(was);
        }

        for (DocWriteRequest<?> req : serverBulk.requests()) {
            builder.addBulkRequestBody(toRequestBody(req));
        }

        return builder.build();
    }

    private static org.opensearch.protobufs.BulkRequestBody toRequestBody(DocWriteRequest<?> req) {
        org.opensearch.protobufs.BulkRequestBody.Builder body = org.opensearch.protobufs.BulkRequestBody.newBuilder();
        org.opensearch.protobufs.OperationContainer.Builder container = org.opensearch.protobufs.OperationContainer.newBuilder();

        if (req instanceof IndexRequest) {
            IndexRequest indexReq = (IndexRequest) req;
            if (indexReq.opType() == DocWriteRequest.OpType.CREATE) {
                if (hasVersionControl(indexReq)) {
                    container.setIndex(toIndexOperation(indexReq, true));
                } else {
                    container.setCreate(toCreateWriteOperation(indexReq));
                }
            } else {
                container.setIndex(toIndexOperation(indexReq, false));
            }
            body.setOperationContainer(container);
            attachSource(body, indexReq.source());
        } else if (req instanceof UpdateRequest) {
            UpdateRequest updateReq = (UpdateRequest) req;
            container.setUpdate(toUpdateOperation(updateReq));
            body.setOperationContainer(container);
            body.setUpdateAction(buildUpdateAction(updateReq));
            // The forward parser reads update doc bytes from BulkRequestBody.object and ignores
            // UpdateAction.doc; mirror that here so the round trip is clean.
            if (updateReq.doc() != null && updateReq.doc().source() != null) {
                body.setObject(toByteString(updateReq.doc().source()));
            }
        } else if (req instanceof DeleteRequest) {
            DeleteRequest deleteReq = (DeleteRequest) req;
            container.setDelete(toDeleteOperation(deleteReq));
            body.setOperationContainer(container);
        } else {
            throw new IllegalStateException("unsupported DocWriteRequest type: " + req.getClass());
        }

        return body.build();
    }

    private static boolean hasVersionControl(IndexRequest req) {
        // For CREATE op type with no explicit version, IndexRequest.version() returns
        // Versions.MATCH_DELETED rather than MATCH_ANY (see IndexRequest#resolveVersionDefaults).
        // Treat both as "no explicit version requested".
        long v = req.version();
        boolean explicitVersion = v != org.opensearch.common.lucene.uid.Versions.MATCH_ANY
            && v != org.opensearch.common.lucene.uid.Versions.MATCH_DELETED;
        return explicitVersion
            || (req.versionType() != null && req.versionType() != VersionType.INTERNAL)
            || req.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
            || req.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    }

    private static org.opensearch.protobufs.IndexOperation toIndexOperation(IndexRequest req, boolean isCreate) {
        org.opensearch.protobufs.IndexOperation.Builder b = org.opensearch.protobufs.IndexOperation.newBuilder();

        if (req.index() != null) b.setXIndex(req.index());
        if (req.id() != null) b.setXId(req.id());
        if (req.routing() != null) b.setRouting(req.routing());
        if (req.getPipeline() != null) b.setPipeline(req.getPipeline());
        if (req.isRequireAlias()) b.setRequireAlias(true);
        if (isCreate) b.setOpType(org.opensearch.protobufs.OpType.OP_TYPE_CREATE);

        long v = req.version();
        if (v != org.opensearch.common.lucene.uid.Versions.MATCH_ANY && v != org.opensearch.common.lucene.uid.Versions.MATCH_DELETED) {
            b.setVersion(v);
        }
        org.opensearch.protobufs.VersionType vt = toProtoVersionType(req.versionType());
        if (vt != null) b.setVersionType(vt);
        if (req.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) b.setIfSeqNo(req.ifSeqNo());
        if (req.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) b.setIfPrimaryTerm(req.ifPrimaryTerm());

        return b.build();
    }

    private static org.opensearch.protobufs.WriteOperation toCreateWriteOperation(IndexRequest req) {
        org.opensearch.protobufs.WriteOperation.Builder b = org.opensearch.protobufs.WriteOperation.newBuilder();

        if (req.index() != null) b.setXIndex(req.index());
        if (req.id() != null) b.setXId(req.id());
        if (req.routing() != null) b.setRouting(req.routing());
        if (req.getPipeline() != null) b.setPipeline(req.getPipeline());
        if (req.isRequireAlias()) b.setRequireAlias(true);
        // WriteOperation has no version / versionType / ifSeqNo / ifPrimaryTerm in the proto schema.

        return b.build();
    }

    private static org.opensearch.protobufs.UpdateOperation toUpdateOperation(UpdateRequest req) {
        org.opensearch.protobufs.UpdateOperation.Builder b = org.opensearch.protobufs.UpdateOperation.newBuilder();

        if (req.index() != null) b.setXIndex(req.index());
        if (req.id() != null) b.setXId(req.id());
        if (req.routing() != null) b.setRouting(req.routing());
        if (req.isRequireAlias()) b.setRequireAlias(true);
        if (req.retryOnConflict() > 0) b.setRetryOnConflict(req.retryOnConflict());

        if (req.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) b.setIfSeqNo(req.ifSeqNo());
        if (req.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) b.setIfPrimaryTerm(req.ifPrimaryTerm());

        return b.build();
    }

    private static org.opensearch.protobufs.DeleteOperation toDeleteOperation(DeleteRequest req) {
        org.opensearch.protobufs.DeleteOperation.Builder b = org.opensearch.protobufs.DeleteOperation.newBuilder();

        if (req.index() != null) b.setXIndex(req.index());
        if (req.id() != null) b.setXId(req.id());
        if (req.routing() != null) b.setRouting(req.routing());

        if (req.version() != org.opensearch.common.lucene.uid.Versions.MATCH_ANY) {
            b.setVersion(req.version());
        }
        org.opensearch.protobufs.VersionType vt = toProtoVersionType(req.versionType());
        if (vt != null) b.setVersionType(vt);
        if (req.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) b.setIfSeqNo(req.ifSeqNo());
        if (req.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) b.setIfPrimaryTerm(req.ifPrimaryTerm());

        return b.build();
    }

    private static void attachSource(org.opensearch.protobufs.BulkRequestBody.Builder body, BytesReference source) {
        if (source != null && source.length() > 0) {
            body.setObject(toByteString(source));
        }
    }

    private static org.opensearch.protobufs.UpdateAction buildUpdateAction(UpdateRequest req) {
        org.opensearch.protobufs.UpdateAction.Builder action = org.opensearch.protobufs.UpdateAction.newBuilder();

        // Note: doc bytes go on BulkRequestBody.object, not UpdateAction.doc; see toRequestBody.
        if (req.upsertRequest() != null && req.upsertRequest().source() != null) {
            action.setUpsert(toByteString(req.upsertRequest().source()));
        }
        if (req.docAsUpsert()) action.setDocAsUpsert(true);
        if (req.scriptedUpsert()) action.setScriptedUpsert(true);
        if (req.detectNoop() == false) action.setDetectNoop(false); // server default is true
        // Script support deferred — the Script proto is a non-trivial subgraph.

        return action.build();
    }

    private static ByteString toByteString(BytesReference bytes) {
        BytesArray ba = (bytes instanceof BytesArray) ? (BytesArray) bytes : new BytesArray(bytes.toBytesRef());
        return ByteString.copyFrom(ba.array(), ba.offset(), ba.length());
    }

    private static org.opensearch.protobufs.Refresh toProtoRefresh(RefreshPolicy policy) {
        if (policy == null || policy == RefreshPolicy.NONE) return null;
        switch (policy) {
            case IMMEDIATE:
                return org.opensearch.protobufs.Refresh.REFRESH_TRUE;
            case WAIT_UNTIL:
                return org.opensearch.protobufs.Refresh.REFRESH_WAIT_FOR;
            default:
                return null;
        }
    }

    private static org.opensearch.protobufs.VersionType toProtoVersionType(VersionType vt) {
        if (vt == null || vt == VersionType.INTERNAL) return null;
        switch (vt) {
            case EXTERNAL:
                return org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL;
            case EXTERNAL_GTE:
                return org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL_GTE;
            default:
                return null;
        }
    }

    private static org.opensearch.protobufs.WaitForActiveShards toProtoWaitForActiveShards(ActiveShardCount count) {
        if (count == null || count == ActiveShardCount.DEFAULT) return null;
        org.opensearch.protobufs.WaitForActiveShards.Builder b = org.opensearch.protobufs.WaitForActiveShards.newBuilder();
        if (count == ActiveShardCount.ALL) {
            b.setOption(org.opensearch.protobufs.WaitForActiveShardOptions.WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL);
        } else {
            b.setCount(parseActiveShardCount(count));
        }
        return b.build();
    }

    private static int parseActiveShardCount(ActiveShardCount count) {
        if (count == ActiveShardCount.NONE) return 0;
        if (count == ActiveShardCount.ONE) return 1;
        try {
            return Integer.parseInt(count.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unexpected ActiveShardCount: " + count, e);
        }
    }
}
