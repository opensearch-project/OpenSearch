/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.multipart.transfer;

import org.opensearch.repositories.s3.SocketAccess;

import java.util.concurrent.Callable;

public class PrivilegedCallable<T> implements Callable<T> {

    private final Callable<T> actualCallable;

    public PrivilegedCallable(Callable<T> actualCallable) {
        this.actualCallable = actualCallable;
    }

    @Override
    public T call() throws Exception {
        return SocketAccess.doPrivilegedException(actualCallable::call);
    }
}
