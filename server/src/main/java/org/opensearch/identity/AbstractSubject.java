/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.util.concurrent.Callable;

/**
 * An AbstractSubject provides a default implementation for runAs simply calls the Callable passed to it
 */
public abstract class AbstractSubject implements Subject {
    public AbstractSubject() {}

    @Override
    public void runAs(Callable<Void> callable) throws Exception {
        callable.call();
    }
}
