/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

public class OrdinalGenerator {
    private int currentOrdinal;

    public static final OrdinalGenerator INSTANCE = new OrdinalGenerator();

    public static OrdinalGenerator getInstance() {
        return INSTANCE;
    }

    public void initialize(int value) {
        this.currentOrdinal = value;
    }

    public int nextOrdinal() {
        return ++currentOrdinal;
    }
}
