/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.stream;

import java.util.Collections;
import java.util.Map;

public class Tuple {

    private final int docId;
    private final Map<String, Object> fields;
    public boolean EOF;

    public Tuple() {
        EOF = true;
        docId = -1;
        fields = Collections.emptyMap();
    }

    public static Tuple newEOF() {
        return new Tuple();
    }

    public Tuple(int docId, Map<String, Object> fields){
        this.docId = docId;
        this.fields = fields;
    }

    public int getDocId() {
        return docId;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return "Tuple{docId=" + docId + ", fields=" + fields + "}";
    }
}
