/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class CompositeIndexWriterForUpdateAndDeletesTests extends CriteriaBasedCompositeIndexWriterBaseTests {

    public void testDeleteWithDocumentInParentWriter() throws IOException {
        final String id = "test";
        CompositeIndexWriter compositeIndexWriter = null;
        try {
            compositeIndexWriter = new CompositeIndexWriter(
                config(),
                createWriter(),
                newSoftDeletesPolicy(),
                softDeletesField,
                indexWriterFactory
            );
            Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            compositeIndexWriter.deleteDocument(
                operation.uid(),
                false,
                newDeleteTombstoneDoc(id),
                1,
                2,
                primaryTerm.get(),
                softDeletesField
            );

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
                assertEquals(0, directoryReader.numDocs());
            }
        } finally {
            if (compositeIndexWriter != null) {
                IOUtils.closeWhileHandlingException(compositeIndexWriter);
            }
        }
    }

    public void testDeleteWithDocumentInChildWriter() throws IOException {
        final String id = "test";
        CompositeIndexWriter compositeIndexWriter = null;
        try {
            compositeIndexWriter = new CompositeIndexWriter(
                config(),
                createWriter(),
                newSoftDeletesPolicy(),
                softDeletesField,
                indexWriterFactory
            );
            Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());
            compositeIndexWriter.deleteDocument(
                operation.uid(),
                false,
                newDeleteTombstoneDoc(id),
                1,
                2,
                primaryTerm.get(),
                softDeletesField
            );

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);

            try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
                assertEquals(0, directoryReader.numDocs());
            }
        } finally {
            if (compositeIndexWriter != null) {
                IOUtils.closeWhileHandlingException(compositeIndexWriter);
            }
        }
    }

    public void testDeleteWithDocumentInBothChildAndParentWriter() throws IOException {
        final String id = "test";
        CompositeIndexWriter compositeIndexWriter = null;
        try {
            compositeIndexWriter = new CompositeIndexWriter(
                config(),
                createWriter(),
                newSoftDeletesPolicy(),
                softDeletesField,
                indexWriterFactory
            );
            Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);

            operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
            compositeIndexWriter.softUpdateDocuments(operation.uid(), operation.docs(), 2, 2, primaryTerm.get(), softDeletesField);
            compositeIndexWriter.deleteDocument(
                operation.uid(),
                false,
                newDeleteTombstoneDoc(id),
                1,
                2,
                primaryTerm.get(),
                softDeletesField
            );

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
                assertEquals(0, directoryReader.numDocs());
            }
        } finally {
            if (compositeIndexWriter != null) {
                IOUtils.closeWhileHandlingException(compositeIndexWriter);
            }
        }
    }

    public void testDeleteWithDocumentInOldChildWriter() throws IOException, InterruptedException {
        final String id = "test";
        CompositeIndexWriter compositeIndexWriter = new CompositeIndexWriter(
            config(),
            createWriter(),
            newSoftDeletesPolicy(),
            softDeletesField,
            indexWriterFactory
        );

        Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
        compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

        CompositeIndexWriter.CriteriaBasedIndexWriterLookup lock = compositeIndexWriter.acquireNewReadLock();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean run = new AtomicBoolean(true);
        Thread refresher = new Thread(() -> {
            latch.countDown();
            try {
                compositeIndexWriter.beforeRefresh();
            } catch (Exception ignored) {}
        });

        refresher.start();
        try {
            latch.await();
            compositeIndexWriter.deleteDocument(
                operation.uid(),
                false,
                newDeleteTombstoneDoc(id),
                1,
                2,
                primaryTerm.get(),
                softDeletesField
            );
        } finally {
            IOUtils.closeWhileHandlingException(lock.getMapReadLock());
            run.set(false);
            refresher.join();
            compositeIndexWriter.afterRefresh(true);
            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
                assertEquals(0, directoryReader.numDocs());
            }

            IOUtils.closeWhileHandlingException(compositeIndexWriter);
        }
    }

    public void testUpdateWithDocumentInParentIndexWriter() throws IOException {
        final String id = "test";
        CompositeIndexWriter compositeIndexWriter = null;
        try {
            compositeIndexWriter = new CompositeIndexWriter(
                config(),
                createWriter(),
                newSoftDeletesPolicy(),
                softDeletesField,
                indexWriterFactory
            );
            Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));

            compositeIndexWriter.softUpdateDocuments(operation.uid(), operation.docs(), 2, 2, primaryTerm.get(), softDeletesField);

            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
                assertEquals(1, directoryReader.numDocs());
            }
        } finally {
            if (compositeIndexWriter != null) {
                IOUtils.closeWhileHandlingException(compositeIndexWriter);
            }
        }
    }

    public void testUpdateWithDocumentInChildIndexWriter() throws IOException {
        final String id = "test";
        CompositeIndexWriter compositeIndexWriter = null;
        try {
            compositeIndexWriter = new CompositeIndexWriter(
                config(),
                createWriter(),
                newSoftDeletesPolicy(),
                softDeletesField,
                indexWriterFactory
            );
            Engine.Index operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
            compositeIndexWriter.addDocuments(operation.docs(), operation.uid());

            operation = indexForDoc(createParsedDoc(id, null, DEFAULT_CRITERIA));
            compositeIndexWriter.softUpdateDocuments(operation.uid(), operation.docs(), 2, 2, primaryTerm.get(), softDeletesField);
            compositeIndexWriter.beforeRefresh();
            compositeIndexWriter.afterRefresh(true);
            try (DirectoryReader directoryReader = DirectoryReader.open(compositeIndexWriter.getAccumulatingIndexWriter())) {
                assertEquals(1, directoryReader.numDocs());
            }
        } finally {
            if (compositeIndexWriter != null) {
                IOUtils.close(compositeIndexWriter);
            }
        }
    }

}
