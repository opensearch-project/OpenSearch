/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.listener;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TranslogListenerTests extends OpenSearchTestCase {

    public void testCompositeTranslogEventListener() {
        AtomicInteger onTranslogSyncInvoked = new AtomicInteger();
        AtomicInteger onTranslogRecoveryInvoked = new AtomicInteger();
        AtomicInteger onBeginTranslogRecoveryInvoked = new AtomicInteger();

        TranslogEventListener listener = new TranslogEventListener() {
            @Override
            public void onTranslogSync() {
                onTranslogSyncInvoked.incrementAndGet();
            }

            @Override
            public void onTranslogRecovery() {
                onTranslogRecoveryInvoked.incrementAndGet();
            }

            @Override
            public void onBeginTranslogRecovery() {
                onBeginTranslogRecoveryInvoked.incrementAndGet();
            }
        };
        TranslogEventListener throwingListener = (TranslogEventListener) Proxy.newProxyInstance(
            TranslogEventListener.class.getClassLoader(),
            new Class[] { TranslogEventListener.class },
            (a, b, c) -> { throw new RuntimeException(); }
        );

        final List<TranslogEventListener> translogEventListeners = new ArrayList<>(Arrays.asList(listener, listener));
        if (randomBoolean()) {
            translogEventListeners.add(throwingListener);
            if (randomBoolean()) {
                translogEventListeners.add(throwingListener);
            }
        }
        Collections.shuffle(translogEventListeners, random());
        TranslogEventListener compositeListener = new CompositeTranslogEventListener(translogEventListeners);
        compositeListener.onTranslogRecovery();
        compositeListener.onTranslogSync();
        compositeListener.onBeginTranslogRecovery();

        assertEquals(2, onBeginTranslogRecoveryInvoked.get());
        assertEquals(2, onTranslogRecoveryInvoked.get());
        assertEquals(2, onTranslogSyncInvoked.get());
    }
}
