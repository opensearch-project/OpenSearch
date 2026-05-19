/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemingestprocessor;

import org.opensearch.ingest.AbstractBatchingSystemProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This system ingest processor will add a field to the ingest doc.
 */
public class ExampleSystemIngestProcessor extends AbstractBatchingSystemProcessor {

    /**
     * Name of the auto added field.
     */
    public static final String AUTO_ADDED_FIELD_NAME = "field_auto_added_by_system_ingest_processor";
    /**
     * Value of the auto added field.
     */
    public static final String AUTO_ADDED_FIELD_VALUE = "This field is auto added by the example system ingest processor.";
    /**
     * The type of the processor.
     */
    public static final String TYPE = "example_system_ingest_processor";

    /**
     * Constructs a new ExampleSystemIngestProcessor
     * @param tag tag of the processor
     * @param description description of the processor
     * @param batchSize batch size which is used to control each batch how many docs the processor can process
     */
    protected ExampleSystemIngestProcessor(String tag, String description, int batchSize) {
        super(tag, description, batchSize);
    }

    @Override
    protected void subBatchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        for (IngestDocumentWrapper ingestDocumentWrapper : ingestDocumentWrappers) {
            this.execute(ingestDocumentWrapper.getIngestDocument());
        }
        handler.accept(ingestDocumentWrappers);
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        super.execute(ingestDocument, handler);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        ingestDocument.setFieldValue(AUTO_ADDED_FIELD_NAME, AUTO_ADDED_FIELD_VALUE);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
