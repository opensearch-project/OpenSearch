/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * While using CRT client we are seeing ThreadLeak for the AwsEventLoop threads. These are Native threads and are
 * initialized one thread per core. We tried to specifically close the thread but couldn't get it terminated.
 * We have opened a git-hub issue  "<a href="https://github.com/awslabs/aws-crt-java/issues/905">...</a>" for the same.
 * Currently, we are using thread filter.
 */
public class EventLoopThreadFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("AwsEventLoop");
    }
}
