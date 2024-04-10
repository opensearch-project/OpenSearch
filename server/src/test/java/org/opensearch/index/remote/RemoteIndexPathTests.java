/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

public class RemoteIndexPathTests extends OpenSearchTestCase {

    public void testToXContent() throws IOException {
        RemoteIndexPath indexPath = new RemoteIndexPath(
            "test",
            5,
            new BlobPath().add("dsd").add("hello"),
            PathType.HASHED_PREFIX,
            PathHashAlgorithm.FNV_1A,
            Map.of(SEGMENTS, List.of(DATA, METADATA, LOCK_FILES))
        );
        XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        xContentBuilder.startObject();
        xContentBuilder = indexPath.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        String expected = "{\"version\":\"1\",\"index_uuid\":\"test\",\"shard_count\":5,\"path_type\":\"HASHED_PREFIX\""
            + ",\"path_hash_algorithm\":\"FNV_1A\",\"paths\":[\"7w7J_t5dXgk/dsd/hello/test/0/segments/data/\","
            + "\"d2tIsVGsh9I/dsd/hello/test/1/segments/data/\",\"jDj7aact8oc/dsd/hello/test/2/segments/data/\","
            + "\"Ea34xvbcE4g/dsd/hello/test/3/segments/data/\",\"EaMx7uStzM0/dsd/hello/test/4/segments/data/\","
            + "\"WR3ZZ6pAH9w/dsd/hello/test/0/segments/metadata/\",\"OmAyQrYTH9c/dsd/hello/test/1/segments/metadata/\","
            + "\"9GFNLB8iu2I/dsd/hello/test/2/segments/metadata/\",\"t4sqq-4QAiU/dsd/hello/test/3/segments/metadata/\","
            + "\"m5IHfRLxuTg/dsd/hello/test/4/segments/metadata/\",\"1l_zRI9-N0I/dsd/hello/test/0/segments/lock_files/\","
            + "\"BJxu8oivE1k/dsd/hello/test/1/segments/lock_files/\",\"mkME0l_DbDA/dsd/hello/test/2/segments/lock_files/\","
            + "\"W7LqEuhiyTc/dsd/hello/test/3/segments/lock_files/\",\"Ls5bj0LgiKY/dsd/hello/test/4/segments/lock_files/\"]}";
        assertEquals(expected, xContentBuilder.toString());
    }
}
