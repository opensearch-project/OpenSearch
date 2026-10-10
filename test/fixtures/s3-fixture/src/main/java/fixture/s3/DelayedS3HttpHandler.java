/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fixture.s3;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayedS3HttpHandler extends S3HttpHandler {

    private final long delayMillis;
    private final String pathPattern;
    private final String methodFilter;
    private final AtomicInteger delayedRequestCount = new AtomicInteger(0);

    public DelayedS3HttpHandler(String bucket, long delayMillis, String methodFilter, String pathPattern) {
        super(bucket);
        this.delayMillis = delayMillis;
        this.methodFilter = methodFilter;
        this.pathPattern = pathPattern;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        final String method = exchange.getRequestMethod();
        final String path = exchange.getRequestURI().getPath();
        if (method.equals(methodFilter) && path.contains(pathPattern)) {
            delayedRequestCount.incrementAndGet();
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        super.handle(exchange);
    }

    public int getDelayedRequestCount() {
        return delayedRequestCount.get();
    }

    public void resetDelayedRequestCount() {
        delayedRequestCount.set(0);
    }
}
