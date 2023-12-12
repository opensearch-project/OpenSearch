/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import org.opensearch.common.annotation.PublicApi;

@PublicApi(since = "1.0.0")
public class PublicApiMethodArgumentNotAnnotatedPackagePrivate {
    public void method(NotAnnotatedPackagePrivate arg) {}
}

// The public API exposes this class through public method argument, it should be annotated and be public
class NotAnnotatedPackagePrivate {}
