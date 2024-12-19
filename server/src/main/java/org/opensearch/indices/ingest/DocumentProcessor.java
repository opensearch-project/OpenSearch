/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.opensearch.index.Message;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.IngestionEngine;

import java.io.IOException;
import java.util.function.Consumer;

public class DocumentProcessor implements Consumer<Message> {
    private final IngestionEngine engine;

    public DocumentProcessor(IngestionEngine engine) {
        this.engine = engine;
    }


    @Override
    public void accept(Message message) {
        Engine.Operation operation = message.getOperation(engine.getDocumentMapperForType());
        try {
            switch (operation.operationType()) {
                case INDEX:
                    engine.index((Engine.Index) operation);
                    break;
                case DELETE:
                    engine.delete((Engine.Delete) operation);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid operation: " + operation);
            }
        } catch (IOException e) {
            // better error handling
            throw new RuntimeException(e);
        }
    }
}
