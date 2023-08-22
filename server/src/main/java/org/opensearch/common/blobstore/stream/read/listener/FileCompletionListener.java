/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.core.action.ActionListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class FileCompletionListener implements ActionListener<Integer> {
    private final int numStreams;
    private final String segmentFileName;
    private final Set<Integer> completedParts;
    private final ActionListener<String> segmentCompletionListener;

    public FileCompletionListener(int numStreams, String segmentFileName, ActionListener<String> segmentCompletionListener) {
        this.completedParts = Collections.synchronizedSet(new HashSet<>());
        this.numStreams = numStreams;
        this.segmentFileName = segmentFileName;
        this.segmentCompletionListener = segmentCompletionListener;
    }

    @Override
    public void onResponse(Integer partNumber) {
        completedParts.add(partNumber);
        if (completedParts.size() == numStreams) {
            segmentCompletionListener.onResponse(segmentFileName);
        }
    }

    @Override
    public void onFailure(Exception e) {
        segmentCompletionListener.onFailure(e);
    }
}
