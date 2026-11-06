/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http;

import org.opensearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Aggregates HTTP requests
 *
 * @opensearch.internal
 */
public class HttpPipeliningAggregator<Listener> {

    private final int maxEventsHeld;
    private final PriorityQueue<Tuple<HttpPipelinedResponse, Listener>> outboundHoldingQueue;
    /*
     * The current read and write sequence numbers. Read sequence numbers are attached to requests in the order they are read from the
     * channel, and then transferred to responses. A response is not written to the channel context until its sequence number matches the
     * current write sequence, implying that all preceding messages have been written.
     */
    private int readSequence;
    private int writeSequence;

    public HttpPipeliningAggregator(int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;
        this.outboundHoldingQueue = new PriorityQueue<>(1, Comparator.comparing(Tuple::v1));
    }

    public HttpPipelinedRequest read(final HttpRequest request) {
        return new HttpPipelinedRequest(readSequence++, request);
    }

    /**
     * Holds a response until it can be polled in sequence. The next-in-sequence response may temporarily exceed the holding limit,
     * including during a reentrant write, since it allows the queue to drain.
     */
    public void add(final HttpPipelinedResponse response, Listener listener) {
        if (maxEventsHeld > 0 && (outboundHoldingQueue.size() < maxEventsHeld || response.getSequence() == writeSequence)) {
            outboundHoldingQueue.add(new Tuple<>(response, listener));
        } else {
            int eventCount = outboundHoldingQueue.size() + 1;
            throw new IllegalStateException("Too many pipelined events [" + eventCount + "]. Max events allowed [" + maxEventsHeld + "].");
        }
    }

    /**
     * Removes only the next ready response, or returns null if an earlier response is still outstanding.
     * Remaining responses stay available for cleanup if the channel closes during a write.
     */
    public Tuple<HttpPipelinedResponse, Listener> poll() {
        final Tuple<HttpPipelinedResponse, Listener> next = outboundHoldingQueue.peek();
        if (next != null && next.v1().getSequence() == writeSequence) {
            outboundHoldingQueue.poll();
            writeSequence++;
            return next;
        }
        return null;
    }

    public List<Tuple<HttpPipelinedResponse, Listener>> removeAllInflightResponses() {
        ArrayList<Tuple<HttpPipelinedResponse, Listener>> responses = new ArrayList<>(outboundHoldingQueue);
        outboundHoldingQueue.clear();
        return responses;
    }
}
