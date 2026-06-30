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

package org.opensearch.indices.recovery;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchWrapperException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

/**
 * Exception thrown if there is an error recovering files
 *
 * @opensearch.internal
 */
public class RecoverFilesRecoveryException extends OpenSearchException implements OpenSearchWrapperException {

    private final int numberOfFiles;

    private final ByteSizeValue totalFilesSize;

    public RecoverFilesRecoveryException(ShardId shardId, int numberOfFiles, ByteSizeValue totalFilesSize, Throwable cause) {
        super("Failed to transfer [{}] files with total size of [{}]", cause, numberOfFiles, totalFilesSize);
        Objects.requireNonNull(totalFilesSize, "totalFilesSize must not be null");
        setShard(shardId);
        this.numberOfFiles = numberOfFiles;
        this.totalFilesSize = totalFilesSize;
    }

    public int numberOfFiles() {
        return numberOfFiles;
    }

    public ByteSizeValue totalFilesSize() {
        return totalFilesSize;
    }

    public RecoverFilesRecoveryException(StreamInput in) throws IOException {
        super(in);
        numberOfFiles = in.readInt();
        totalFilesSize = new ByteSizeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(numberOfFiles);
        totalFilesSize.writeTo(out);
    }
}
