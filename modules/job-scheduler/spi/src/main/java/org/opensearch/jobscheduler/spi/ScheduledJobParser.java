/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi;

import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;

public interface ScheduledJobParser {
    ScheduledJobParameter parse(XContentParser xContentParser, String id, JobDocVersion jobDocVersion) throws IOException;
}
