/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.util.Map;

/**
 * Processor that stops the pipeline it runs in, usually behind an {@code if}. The processors of the pipeline that come after it are
 * skipped, and the document is handled as if the pipeline had finished normally: it is not dropped like it is by the {@code drop}
 * processor, and it is not rejected like it is by the {@code fail} processor. It counts as a success in the ingest stats and adds no
 * {@code on_failure} metadata.
 * <p>
 * It ends only the pipeline that it runs in. A pipeline that called this one with the {@code pipeline} processor carries on, and the
 * {@code final_pipeline} of the index still runs.
 */
public final class TerminateProcessor extends AbstractProcessor {

    public static final String TYPE = "terminate";

    TerminateProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        ingestDocument.terminatePipeline();
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Factory for the processor, which takes no configuration beyond the options that every processor supports.
     */
    public static final class Factory implements Processor.Factory {

        @Override
        public TerminateProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) {
            return new TerminateProcessor(tag, description);
        }
    }
}
