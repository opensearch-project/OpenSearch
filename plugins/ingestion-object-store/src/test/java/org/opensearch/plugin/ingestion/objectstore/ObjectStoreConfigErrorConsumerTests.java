/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ObjectStoreConfigErrorConsumerTests extends OpenSearchTestCase {

    private final IllegalArgumentException cause = new IllegalArgumentException("[format] must be [ndjson] in v0, got [csv]");
    private final ObjectStoreConfigErrorConsumer consumer = new ObjectStoreConfigErrorConsumer(3, cause);

    public void testStatelessReadThrowsWithTheOriginalMessageAndCause() {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getMessage(), containsString("shard [3] is invalid, so ingestion is paused"));
        assertThat(e.getMessage(), containsString("[format] must be [ndjson] in v0, got [csv]"));
        assertThat(e.getCause(), is(sameInstance(cause)));
    }

    public void testPointerReadThrowsWithTheOriginalMessageAndCause() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> consumer.readNext(new ObjectStorePointer("a.ndjson", 5), false, 10, 1000)
        );
        assertThat(e.getMessage(), containsString("[format] must be [ndjson] in v0, got [csv]"));
        assertThat(e.getCause(), is(sameInstance(cause)));
    }

    public void testEveryReadThrowsAgain() {
        expectThrows(IllegalStateException.class, () -> consumer.readNext(10, 1000));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> consumer.readNext(10, 1000));
        assertThat(e.getCause(), is(sameInstance(cause)));
    }

    public void testPointerMethodsThrowTheConfigurationError() {
        assertThat(expectThrows(IllegalStateException.class, consumer::earliestPointer).getCause(), is(sameInstance(cause)));
        assertThat(expectThrows(IllegalStateException.class, consumer::latestPointer).getCause(), is(sameInstance(cause)));
        assertThat(
            expectThrows(IllegalStateException.class, () -> consumer.pointerFromTimestampMillis(1234L)).getCause(),
            is(sameInstance(cause))
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> consumer.pointerFromOffset("not a pointer"));
        assertThat(e.getCause(), is(sameInstance(cause)));
        assertThat(e.getMessage(), containsString("[format] must be [ndjson] in v0, got [csv]"));
    }

    public void testLagIsUnknownRatherThanComplete() {
        assertThat(consumer.getPointerBasedLag(null), is(-1L));
        assertThat(consumer.getPointerBasedLag(new ObjectStorePointer("a.ndjson", 0)), is(ObjectStoreConfigErrorConsumer.UNKNOWN_LAG));
    }

    public void testShardIdAndClose() {
        assertThat(consumer.getShardId(), is(3));
        consumer.close();
        assertThat(consumer.getShardId(), is(3));
    }

    public void testNullErrorRejected() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> new ObjectStoreConfigErrorConsumer(0, null));
        assertThat(e.getMessage(), containsString("error must not be null"));
    }
}
