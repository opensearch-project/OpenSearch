/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.blobstore.stream.read.listener.ListenerTestUtils.CountingCompletionListener;

public class FileCompletionListenerTests extends OpenSearchTestCase {

    public void testFileCompletionListener() {
        int numStreams = 10;
        String fileName = "test_segment_file";
        CountingCompletionListener<String> completionListener = new CountingCompletionListener<String>();
        FileCompletionListener fileCompletionListener = new FileCompletionListener(numStreams, fileName, completionListener);

        for (int stream = 0; stream < numStreams; stream++) {
            // Ensure completion listener called only when all streams are completed
            assertEquals(0, completionListener.getResponseCount());
            fileCompletionListener.onResponse(null);
        }

        assertEquals(1, completionListener.getResponseCount());
        assertEquals(fileName, completionListener.getResponse());
    }

    public void testFileCompletionListenerFailure() {
        int numStreams = 10;
        String fileName = "test_segment_file";
        CountingCompletionListener<String> completionListener = new CountingCompletionListener<String>();
        FileCompletionListener fileCompletionListener = new FileCompletionListener(numStreams, fileName, completionListener);

        // Fail the listener initially
        IOException exception = new IOException();
        fileCompletionListener.onFailure(exception);

        for (int stream = 0; stream < numStreams - 1; stream++) {
            assertEquals(0, completionListener.getResponseCount());
            fileCompletionListener.onResponse(null);
        }

        assertEquals(1, completionListener.getFailureCount());
        assertEquals(exception, completionListener.getException());
        assertEquals(0, completionListener.getResponseCount());

        fileCompletionListener.onFailure(exception);
        assertEquals(2, completionListener.getFailureCount());
        assertEquals(exception, completionListener.getException());
    }
}
