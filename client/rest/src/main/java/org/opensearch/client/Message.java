/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.HttpMessage;

import java.util.Objects;

final class Message<H extends HttpMessage, B> {
    private final H head;
    private final B body;

    public Message(final H head, final B body) {
        this.head = Objects.requireNonNull(head, "Message head");
        this.body = body;
    }

    public H getHead() {
        return head;
    }

    public B getBody() {
        return body;
    }

    @Override
    public String toString() {
        return "[" + "head=" + head + ", body=" + body + ']';
    }
}
