/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

public class Token {
    int type;
    String text;
    int line;

    Token(int type, String text, int line) {
        this.type = type;
        this.text = text;
        this.line = line;
    }
}
