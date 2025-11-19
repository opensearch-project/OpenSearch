/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import com.google.protobuf.ByteString;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequestParser;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.VersionType;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.DeleteOperation;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.OpType;
import org.opensearch.protobufs.OperationContainer;
import org.opensearch.protobufs.UpdateAction;
import org.opensearch.protobufs.UpdateOperation;
import org.opensearch.protobufs.WriteOperation;
import org.opensearch.script.Script;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.transport.grpc.proto.request.common.FetchSourceContextProtoUtils;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.document.common.VersionTypeProtoUtils;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

/**
 * Parses bulk requests.
 *
 */
public class BulkRequestParserProtoUtils {
    // protected final Settings settings;

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    protected BulkRequestParserProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Same as { BulkRequest#valueOrDefault(String, String)
     * @param value
     * @param globalDefault
     * @return
     */
    private static String valueOrDefault(String value, String globalDefault) {
        if (Strings.isNullOrEmpty(value) && Strings.isNullOrEmpty(globalDefault) == false) {
            return globalDefault;
        }
        return value;
    }

    /**
     * Same as { BulkRequest#valueOrDefault(Boolean, Boolean)}
     * @param value
     * @param globalDefault
     * @return
     */
    private static Boolean valueOrDefault(Boolean value, Boolean globalDefault) {
        if (Objects.isNull(value) && !Objects.isNull(globalDefault)) {
            return globalDefault;
        }
        return value;
    }

    /**
     * Converts a protobuf ByteString to OpenSearch BytesReference.
     *
     * This method uses ByteString.toByteArray() which delegates to protobuf's internal
     * implementation. Protobuf optimizes this based on the ByteString's internal representation:
     * - For ByteStrings created with UnsafeByteOperations.unsafeWrap(), it returns the wrapped array directly (zero-copy)
     * - For other ByteStrings, it creates a copy
     *
     * @param byteString The protobuf ByteString to convert
     * @return A BytesReference wrapping the ByteString data
     */
    private static BytesReference byteStringToBytesReference(ByteString byteString) {
        if (byteString == null || byteString.isEmpty()) {
            return BytesArray.EMPTY;
        }
        // Let protobuf handle the conversion efficiently
        // For ByteStrings created with UnsafeByteOperations.unsafeWrap(), this returns the backing array
        return new BytesArray(byteString.toByteArray());
    }

    /**
     * Detects the media type from BytesReference content, with fallback to JSON if detection fails.
     * This enables support for JSON, SMILE, and CBOR formats in gRPC bulk requests.
     *
     * @param document The document content as BytesReference
     * @return The detected MediaType, or JSON if detection fails or document is empty
     */
    static MediaType detectMediaType(BytesReference document) {
        if (document == null || document.length() == 0) {
            return MediaTypeRegistry.JSON;
        }
        BytesRef bytesRef = document.toBytesRef();
        MediaType detectedType = MediaTypeRegistry.mediaTypeFromBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        return detectedType != null ? detectedType : MediaTypeRegistry.JSON;
    }

    /**
     * Similar to {@link BulkRequestParser#parse(BytesReference, String, String, FetchSourceContext, String, Boolean, boolean, MediaType, Consumer, Consumer, Consumer)}, except that it takes into account global values.
     *
     * @param request
     * @param defaultIndex
     * @param defaultRouting
     * @param defaultFetchSourceContext
     * @param defaultPipeline
     * @param defaultRequireAlias
     * @return
     */
    public static DocWriteRequest<?>[] getDocWriteRequests(
        BulkRequest request,
        String defaultIndex,
        String defaultRouting,
        FetchSourceContext defaultFetchSourceContext,
        String defaultPipeline,
        Boolean defaultRequireAlias
    ) {
        List<BulkRequestBody> bulkRequestBodyList = request.getBulkRequestBodyList();
        DocWriteRequest<?>[] docWriteRequests = new DocWriteRequest<?>[bulkRequestBodyList.size()];

        // Process each operation in the request body
        for (int i = 0; i < bulkRequestBodyList.size(); i++) {
            BulkRequestBody bulkRequestBodyEntry = bulkRequestBodyList.get(i);
            DocWriteRequest<?> docWriteRequest;

            // Set default values, taking into account global values, similar to BulkRequest#add(BytesReference, ...., )
            String index = defaultIndex;
            String id = null;
            String routing = valueOrDefault(defaultRouting, request.getRouting());
            FetchSourceContext fetchSourceContext = defaultFetchSourceContext;
            OpType opType = null;
            long version = Versions.MATCH_ANY;
            VersionType versionType = VersionType.INTERNAL;
            long ifSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;
            int retryOnConflict = 0;
            String pipeline = valueOrDefault(defaultPipeline, request.getPipeline());
            Boolean requireAlias = valueOrDefault(defaultRequireAlias, request.getRequireAlias());

            OperationContainer operationContainer = bulkRequestBodyEntry.getOperationContainer();
            switch (operationContainer.getOperationContainerCase()) {
                case CREATE:
                    docWriteRequest = buildCreateRequest(
                        operationContainer.getCreate(),
                        bulkRequestBodyEntry.getObject(),
                        index,
                        id,
                        routing,
                        version,
                        versionType,
                        pipeline,
                        ifSeqNo,
                        ifPrimaryTerm,
                        requireAlias
                    );
                    break;
                case INDEX:
                    docWriteRequest = buildIndexRequest(
                        operationContainer.getIndex(),
                        bulkRequestBodyEntry.getObject(),
                        opType,
                        index,
                        id,
                        routing,
                        version,
                        versionType,
                        pipeline,
                        ifSeqNo,
                        ifPrimaryTerm,
                        requireAlias
                    );
                    break;
                case UPDATE:
                    // Extract the doc field from UpdateAction
                    // Use ByteString directly to avoid unnecessary byte array allocation
                    ByteString updateDocBytes = ByteString.EMPTY;
                    if (bulkRequestBodyEntry.hasUpdateAction() && bulkRequestBodyEntry.getUpdateAction().hasDoc()) {
                        updateDocBytes = bulkRequestBodyEntry.getUpdateAction().getDoc();
                    } else if (bulkRequestBodyEntry.hasObject()) {
                        // Fallback to object field for backwards compatibility
                        // TODO: Remove this fallback once all clients use UpdateAction.doc
                        updateDocBytes = bulkRequestBodyEntry.getObject();
                    }
                    docWriteRequest = buildUpdateRequest(
                        operationContainer.getUpdate(),
                        updateDocBytes,
                        bulkRequestBodyEntry,
                        index,
                        id,
                        routing,
                        fetchSourceContext,
                        retryOnConflict,
                        pipeline,
                        ifSeqNo,
                        ifPrimaryTerm,
                        requireAlias
                    );
                    break;
                case DELETE:
                    docWriteRequest = buildDeleteRequest(
                        operationContainer.getDelete(),
                        index,
                        id,
                        routing,
                        version,
                        versionType,
                        ifSeqNo,
                        ifPrimaryTerm
                    );
                    break;
                case OPERATIONCONTAINER_NOT_SET:
                default:
                    throw new IllegalArgumentException(
                        "Invalid BulkRequestBody. An OperationContainer (create, index, update, or delete) must be provided."
                    );
            }
            // Add the request to the bulk request
            docWriteRequests[i] = docWriteRequest;
        }
        return docWriteRequests;
    }

    /**
     * Builds an IndexRequest with create flag set to true from a CreateOperation protobuf message.
     *
     * @param createOperation The create operation protobuf message
     * @param documentBytes The document content as ByteString (zero-copy reference)
     * @param index The default index name
     * @param id The default document ID
     * @param routing The default routing value
     * @param version The default version
     * @param versionType The default version type
     * @param pipeline The default pipeline
     * @param ifSeqNo The default sequence number for optimistic concurrency control
     * @param ifPrimaryTerm The default primary term for optimistic concurrency control
     * @param requireAlias Whether the index must be an alias
     * @return The constructed IndexRequest
     */
    public static IndexRequest buildCreateRequest(
        WriteOperation createOperation,
        ByteString documentBytes,
        String index,
        String id,
        String routing,
        long version,
        VersionType versionType,
        String pipeline,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAlias
    ) {
        if (createOperation.hasXIndex()) {
            index = createOperation.getXIndex();
        }

        id = createOperation.hasXId() ? createOperation.getXId() : id;
        routing = createOperation.hasRouting() ? createOperation.getRouting() : routing;
        pipeline = createOperation.hasPipeline() ? createOperation.getPipeline() : pipeline;
        requireAlias = createOperation.hasRequireAlias() ? createOperation.getRequireAlias() : requireAlias;

        BytesReference documentRef = byteStringToBytesReference(documentBytes);
        MediaType mediaType = detectMediaType(documentRef);
        IndexRequest indexRequest = new IndexRequest(index).id(id)
            .routing(routing)
            .version(version)
            .versionType(versionType)
            .create(true)
            .setPipeline(pipeline)
            .setIfSeqNo(ifSeqNo)
            .setIfPrimaryTerm(ifPrimaryTerm)
            .source(documentRef, mediaType)
            .setRequireAlias(requireAlias);
        return indexRequest;
    }

    /**
     * Builds an IndexRequest from an IndexOperation protobuf message.
     *
     * @param indexOperation The index operation protobuf message
     * @param documentBytes The document content as ByteString (zero-copy reference)
     * @param opType The default operation type
     * @param index The default index name
     * @param id The default document ID
     * @param routing The default routing value
     * @param version The default version
     * @param versionType The default version type
     * @param pipeline The default pipeline
     * @param ifSeqNo The default sequence number for optimistic concurrency control
     * @param ifPrimaryTerm The default primary term for optimistic concurrency control
     * @param requireAlias Whether the index must be an alias
     * @return The constructed IndexRequest
     */
    public static IndexRequest buildIndexRequest(
        IndexOperation indexOperation,
        ByteString documentBytes,
        OpType opType,
        String index,
        String id,
        String routing,
        long version,
        VersionType versionType,
        String pipeline,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAlias
    ) {
        opType = indexOperation.hasOpType() ? indexOperation.getOpType() : opType;

        if (indexOperation.hasXIndex()) {
            index = indexOperation.getXIndex();
        }

        id = indexOperation.hasXId() ? indexOperation.getXId() : id;
        routing = indexOperation.hasRouting() ? indexOperation.getRouting() : routing;
        version = indexOperation.hasVersion() ? indexOperation.getVersion() : version;
        if (indexOperation.hasVersionType()) {
            versionType = VersionTypeProtoUtils.fromProto(indexOperation.getVersionType());
        }
        pipeline = indexOperation.hasPipeline() ? indexOperation.getPipeline() : pipeline;
        ifSeqNo = indexOperation.hasIfSeqNo() ? indexOperation.getIfSeqNo() : ifSeqNo;
        ifPrimaryTerm = indexOperation.hasIfPrimaryTerm() ? indexOperation.getIfPrimaryTerm() : ifPrimaryTerm;
        requireAlias = indexOperation.hasRequireAlias() ? indexOperation.getRequireAlias() : requireAlias;

        BytesReference documentRef = byteStringToBytesReference(documentBytes);
        MediaType mediaType = detectMediaType(documentRef);
        IndexRequest indexRequest;
        if (opType == null) {
            indexRequest = new IndexRequest(index).id(id)
                .routing(routing)
                .version(version)
                .versionType(versionType)
                .setPipeline(pipeline)
                .setIfSeqNo(ifSeqNo)
                .setIfPrimaryTerm(ifPrimaryTerm)
                .source(documentRef, mediaType)
                .setRequireAlias(requireAlias);
        } else {
            indexRequest = new IndexRequest(index).id(id)
                .routing(routing)
                .version(version)
                .versionType(versionType)
                .create(opType == OpType.OP_TYPE_CREATE)
                .setPipeline(pipeline)
                .setIfSeqNo(ifSeqNo)
                .setIfPrimaryTerm(ifPrimaryTerm)
                .source(documentRef, mediaType)
                .setRequireAlias(requireAlias);
        }
        return indexRequest;
    }

    /**
     * Builds an UpdateRequest from an UpdateOperation protobuf message.
     *
     * @param updateOperation The update operation protobuf message
     * @param documentBytes The document content as ByteString (zero-copy reference)
     * @param bulkRequestBody The bulk request body containing additional update options
     * @param index The default index name
     * @param id The default document ID
     * @param routing The default routing value
     * @param fetchSourceContext The default fetch source context
     * @param retryOnConflict The default number of retries on conflict
     * @param pipeline The default pipeline
     * @param ifSeqNo The default sequence number for optimistic concurrency control
     * @param ifPrimaryTerm The default primary term for optimistic concurrency control
     * @param requireAlias Whether the index must be an alias
     * @return The constructed UpdateRequest
     */
    public static UpdateRequest buildUpdateRequest(
        UpdateOperation updateOperation,
        ByteString documentBytes,
        BulkRequestBody bulkRequestBody,
        String index,
        String id,
        String routing,
        FetchSourceContext fetchSourceContext,
        int retryOnConflict,
        String pipeline,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAlias
    ) {
        if (updateOperation.hasXIndex()) {
            index = updateOperation.getXIndex();
        }

        id = updateOperation.hasXId() ? updateOperation.getXId() : id;
        routing = updateOperation.hasRouting() ? updateOperation.getRouting() : routing;
        retryOnConflict = updateOperation.hasRetryOnConflict() ? updateOperation.getRetryOnConflict() : retryOnConflict;
        ifSeqNo = updateOperation.hasIfSeqNo() ? updateOperation.getIfSeqNo() : ifSeqNo;
        ifPrimaryTerm = updateOperation.hasIfPrimaryTerm() ? updateOperation.getIfPrimaryTerm() : ifPrimaryTerm;
        requireAlias = updateOperation.hasRequireAlias() ? updateOperation.getRequireAlias() : requireAlias;

        // Create UpdateRequest with operation-level fields
        UpdateRequest updateRequest = new UpdateRequest().index(index)
            .id(id)
            .routing(routing)
            .retryOnConflict(retryOnConflict)
            .setIfSeqNo(ifSeqNo)
            .setIfPrimaryTerm(ifPrimaryTerm)
            .setRequireAlias(requireAlias)
            .routing(routing);

        // Populate all document-level fields
        updateRequest = fromProto(updateRequest, documentBytes, bulkRequestBody, ifSeqNo, ifPrimaryTerm);

        // Apply fetchSourceContext default
        if (fetchSourceContext != null) {
            updateRequest.fetchSource(fetchSourceContext);
        }

        // Set pipeline on upsert request if it exists
        IndexRequest upsertRequest = updateRequest.upsertRequest();
        if (upsertRequest != null) {
            upsertRequest.setPipeline(pipeline);
        }

        return updateRequest;
    }

    /**
     * Populates an UpdateRequest with values from protobuf messages.
     * Equivalent to {@link UpdateRequest#fromXContent(XContentParser)} for REST API.
     *
     * @param updateRequest The update request to populate (may already have if_seq_no/if_primary_term set)
     * @param documentBytes The document content as ByteString (zero-copy reference)
     * @param bulkRequestBody The bulk request body containing update options
     * @param ifSeqNoFromOperation The sequence number
     * @param ifPrimaryTermFromOperation The primary term
     * @return The populated UpdateRequest
     */
    static UpdateRequest fromProto(
        UpdateRequest updateRequest,
        ByteString documentBytes,
        BulkRequestBody bulkRequestBody,
        long ifSeqNoFromOperation,
        long ifPrimaryTermFromOperation
    ) {
        // Start with operation metadata values
        long ifSeqNo = ifSeqNoFromOperation;
        long ifPrimaryTerm = ifPrimaryTermFromOperation;
        if (bulkRequestBody.hasUpdateAction()) {
            UpdateAction updateAction = bulkRequestBody.getUpdateAction();

            // 1. script
            if (updateAction.hasScript()) {
                Script script = ScriptProtoUtils.parseFromProtoRequest(updateAction.getScript());
                updateRequest.script(script);
            }

            // 2. scripted_upsert
            if (updateAction.hasScriptedUpsert()) {
                updateRequest.scriptedUpsert(updateAction.getScriptedUpsert());
            }

            // 3. upsert
            if (updateAction.hasUpsert()) {
                ByteString upsertBytes = updateAction.getUpsert();
                BytesReference upsertRef = byteStringToBytesReference(upsertBytes);
                MediaType upsertMediaType = detectMediaType(upsertRef);
                BytesRef bytesRef = upsertRef.toBytesRef();
                updateRequest.upsert(bytesRef.bytes, bytesRef.offset, bytesRef.length, upsertMediaType);
            }
        }

        // 4. doc
        // Only set doc if ByteString is non-empty (empty ByteString = field not provided in proto)
        // This check is structural, not business validation:
        // - ByteString.EMPTY = no doc field in proto → don't call doc() → keeps doc=null
        // - Non-empty ByteString = doc field in proto → call doc() → sets doc!=null
        // UpdateRequest.validate() then handles business rules:
        // - If script!=null && doc!=null → validation error
        // - If script==null && doc==null → validation error
        if (documentBytes != null && !documentBytes.isEmpty()) {
            BytesReference docRef = byteStringToBytesReference(documentBytes);
            MediaType mediaType = detectMediaType(docRef);
            BytesRef bytesRef = docRef.toBytesRef();
            updateRequest.doc(bytesRef.bytes, bytesRef.offset, bytesRef.length, mediaType);
        }

        if (bulkRequestBody.hasUpdateAction()) {
            UpdateAction updateAction = bulkRequestBody.getUpdateAction();

            // 5. doc_as_upsert
            if (updateAction.hasDocAsUpsert()) {
                updateRequest.docAsUpsert(updateAction.getDocAsUpsert());
            }

            // 6. detect_noop
            if (updateAction.hasDetectNoop()) {
                updateRequest.detectNoop(updateAction.getDetectNoop());
            }

            // 7. _source
            if (updateAction.hasXSource()) {
                updateRequest.fetchSource(FetchSourceContextProtoUtils.fromProto(updateAction.getXSource()));
            }

            // 8 + 9. if_seq_no and if_primary_term are excluded from UpdateAction protobufs intentionally, as users can just provide them
            // in UpdateOperation.
        }

        // Set if_seq_no and if_primary_term
        updateRequest.setIfSeqNo(ifSeqNo);
        updateRequest.setIfPrimaryTerm(ifPrimaryTerm);

        return updateRequest;
    }

    /**
     * Builds a DeleteRequest from a DeleteOperation protobuf message.
     *
     * @param deleteOperation The delete operation protobuf message
     * @param index The default index name
     * @param id The default document ID
     * @param routing The default routing value
     * @param version The default version
     * @param versionType The default version type
     * @param ifSeqNo The default sequence number for optimistic concurrency control
     * @param ifPrimaryTerm The default primary term for optimistic concurrency control
     * @return The constructed DeleteRequest
     */
    public static DeleteRequest buildDeleteRequest(
        DeleteOperation deleteOperation,
        String index,
        String id,
        String routing,
        long version,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        if (deleteOperation.hasXIndex()) {
            index = deleteOperation.getXIndex();
        }

        id = deleteOperation.hasXId() ? deleteOperation.getXId() : id;
        routing = deleteOperation.hasRouting() ? deleteOperation.getRouting() : routing;
        version = deleteOperation.hasVersion() ? deleteOperation.getVersion() : version;
        if (deleteOperation.hasVersionType()) {
            versionType = VersionTypeProtoUtils.fromProto(deleteOperation.getVersionType());
        }
        ifSeqNo = deleteOperation.hasIfSeqNo() ? deleteOperation.getIfSeqNo() : ifSeqNo;
        ifPrimaryTerm = deleteOperation.hasIfPrimaryTerm() ? deleteOperation.getIfPrimaryTerm() : ifPrimaryTerm;

        DeleteRequest deleteRequest = new DeleteRequest(index).id(id)
            .routing(routing)
            .version(version)
            .versionType(versionType)
            .setIfSeqNo(ifSeqNo)
            .setIfPrimaryTerm(ifPrimaryTerm);

        return deleteRequest;
    }
}
