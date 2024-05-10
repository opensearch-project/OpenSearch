/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

/**
 * A IngestDocument wrapper including the slot of the IngestDocument in original IndexRequests.
 * It also stores the exception happened during ingest process of the document.
 */
public final class IngestDocumentWrapper {
    private final int slot;
    private IngestDocument ingestDocument;
    private Exception exception;

    public IngestDocumentWrapper(int slot, IngestDocument ingestDocument, Exception ex) {
        this.slot = slot;
        this.ingestDocument = ingestDocument;
        this.exception = ex;
    }

    public int getSlot() {
        return this.slot;
    }

    public IngestDocument getIngestDocument() {
        return this.ingestDocument;
    }

    public Exception getException() {
        return this.exception;
    }

    public void update(IngestDocument result, Exception ex) {
        this.ingestDocument = result;
        this.exception = ex;
    }
}
