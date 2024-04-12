/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.index.VersionType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a helper class for static functions which can live outside IngestService.
 */
public class IngestServiceHelper {
    private IngestServiceHelper() {}

    public static void updateIndexRequestWithIngestDocument(IndexRequest indexRequest, IngestDocument ingestDocument) {
        if (ingestDocument == null || indexRequest == null) {
            return;
        }
        Map<IngestDocument.Metadata, Object> metadataMap = ingestDocument.extractMetadata();
        // it's fine to set all metadata fields all the time, as ingest document holds their starting values
        // before ingestion, which might also get modified during ingestion.
        indexRequest.index((String) metadataMap.get(IngestDocument.Metadata.INDEX));
        indexRequest.id((String) metadataMap.get(IngestDocument.Metadata.ID));
        indexRequest.routing((String) metadataMap.get(IngestDocument.Metadata.ROUTING));
        indexRequest.version(((Number) metadataMap.get(IngestDocument.Metadata.VERSION)).longValue());
        if (metadataMap.get(IngestDocument.Metadata.VERSION_TYPE) != null) {
            indexRequest.versionType(VersionType.fromString((String) metadataMap.get(IngestDocument.Metadata.VERSION_TYPE)));
        }
        if (metadataMap.get(IngestDocument.Metadata.IF_SEQ_NO) != null) {
            indexRequest.setIfSeqNo(((Number) metadataMap.get(IngestDocument.Metadata.IF_SEQ_NO)).longValue());
        }
        if (metadataMap.get(IngestDocument.Metadata.IF_PRIMARY_TERM) != null) {
            indexRequest.setIfPrimaryTerm(((Number) metadataMap.get(IngestDocument.Metadata.IF_PRIMARY_TERM)).longValue());
        }
        indexRequest.source(ingestDocument.getSourceAndMetadata(), indexRequest.getContentType());
    }


    public static IngestDocument toIngestDocument(IndexRequest indexRequest) {
        return new IngestDocument(indexRequest.index(), indexRequest.id(), indexRequest.routing(),
            indexRequest.version(), indexRequest.versionType(), indexRequest.sourceAsMap());
    }

    public static IngestDocumentWrapper toIngestDocumentWrapper(int slot, IndexRequest indexRequest) {
        return new IngestDocumentWrapper(slot, toIngestDocument(indexRequest), null);
    }

    public static Map<Integer, IndexRequest> createSlotIndexRequestMap(List<Integer> slots,
        List<IndexRequest> indexRequests) {
        Map<Integer, IndexRequest> slotIndexRequestMap = new HashMap<>();
        for (int i = 0; i < slots.size(); ++i) {
            slotIndexRequestMap.put(slots.get(i), indexRequests.get(i));
        }
        return slotIndexRequestMap;
    }
}



