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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RemoteIndexPathTests extends OpenSearchTestCase {

    /**
     * This checks that the remote path contains paths only for segment and data/metadata/lock_files combination.
     */
    public void testToXContentWithSegmentRepo() throws IOException {
        RemoteIndexPath indexPath = new RemoteIndexPath(
            "djjsid73he8yd7usduh",
            2,
            new BlobPath().add("djsd878ndjh").add("hcs87cj8"),
            PathType.HASHED_PREFIX,
            PathHashAlgorithm.FNV_1A,
            RemoteIndexPath.SEGMENT_PATH
        );
        XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        xContentBuilder.startObject();
        xContentBuilder = indexPath.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        String expected = "{\"version\":\"1\",\"index_uuid\":\"djjsid73he8yd7usduh\",\"shard_count\":2,"
            + "\"path_type\":\"HASHED_PREFIX\",\"path_hash_algorithm\":\"FNV_1A\",\"paths\":["
            + "\"9BmBinD5HYs/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/0/segments/data/\","
            + "\"ExCNOD8_5ew/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/1/segments/data/\","
            + "\"z8wtf0yr2l4/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/0/segments/metadata/\","
            + "\"VheHVwFlExE/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/1/segments/metadata/\","
            + "\"IgFKbsDeUpQ/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/0/segments/lock_files/\","
            + "\"pA3gy_GZtns/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/1/segments/lock_files/\"]}";
        assertEquals(expected, xContentBuilder.toString());
    }

    /**
     * This checks that the remote path contains paths only for translog and data/metadata combination.
     */
    public void testToXContentForTranslogRepoOnly() throws IOException {
        RemoteIndexPath indexPath = new RemoteIndexPath(
            "djjsid73he8yd7usduh",
            2,
            new BlobPath().add("djsd878ndjh").add("hcs87cj8"),
            PathType.HASHED_PREFIX,
            PathHashAlgorithm.FNV_1A,
            RemoteIndexPath.TRANSLOG_PATH
        );
        XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        xContentBuilder.startObject();
        xContentBuilder = indexPath.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        String expected = "{\"version\":\"1\",\"index_uuid\":\"djjsid73he8yd7usduh\",\"shard_count\":2,"
            + "\"path_type\":\"HASHED_PREFIX\",\"path_hash_algorithm\":\"FNV_1A\",\"paths\":["
            + "\"2EaVODaKBck/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/0/translog/data/\","
            + "\"dTS2VqEOUNo/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/1/translog/data/\","
            + "\"PVNKNGonmZw/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/0/translog/metadata/\","
            + "\"NXmt0Y6NjA8/djsd878ndjh/hcs87cj8/djjsid73he8yd7usduh/1/translog/metadata/\"]}";
        assertEquals(expected, xContentBuilder.toString());
    }

    /**
     * This checks that the remote path contains paths only for translog and data/metadata combination.
     */
    public void testToXContentForBothRepos() throws IOException {
        Map<RemoteStoreEnums.DataCategory, List<RemoteStoreEnums.DataType>> pathCreationMap = new TreeMap<>();
        pathCreationMap.putAll(RemoteIndexPath.TRANSLOG_PATH);
        pathCreationMap.putAll(RemoteIndexPath.SEGMENT_PATH);
        RemoteIndexPath indexPath = new RemoteIndexPath(
            "csbdqiu8a7sdnjdks",
            3,
            new BlobPath().add("nxf9yv0").add("c3ejoi"),
            PathType.HASHED_PREFIX,
            PathHashAlgorithm.FNV_1A,
            pathCreationMap
        );
        XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        xContentBuilder.startObject();
        xContentBuilder = indexPath.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();
        String expected = "{\"version\":\"1\",\"index_uuid\":\"csbdqiu8a7sdnjdks\",\"shard_count\":3,\"path_type\":"
            + "\"HASHED_PREFIX\",\"path_hash_algorithm\":\"FNV_1A\",\"paths\":["
            + "\"Cjo0F6kNjYk/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/0/segments/data/\","
            + "\"kpayyhxct1I/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/1/segments/data/\","
            + "\"p2RlgnHeIgc/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/2/segments/data/\","
            + "\"gkPIurBtB1w/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/0/segments/metadata/\","
            + "\"Y4YhlbxAB1c/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/1/segments/metadata/\","
            + "\"HYc8fyVPouI/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/2/segments/metadata/\","
            + "\"igzyZCz1ysI/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/0/segments/lock_files/\","
            + "\"uEluEiYmptk/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/1/segments/lock_files/\","
            + "\"TfAD8f06_7A/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/2/segments/lock_files/\","
            + "\"QqKEpasbEGs/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/0/translog/data/\","
            + "\"sNyoimoe1Bw/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/1/translog/data/\","
            + "\"d4YQtONfq50/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/2/translog/data/\","
            + "\"zLr4UXjK8T4/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/0/translog/metadata/\","
            + "\"_s8i7ZmlXGE/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/1/translog/metadata/\","
            + "\"tvtD3-k5ISg/nxf9yv0/c3ejoi/csbdqiu8a7sdnjdks/2/translog/metadata/\"]}";
        assertEquals(expected, xContentBuilder.toString());
    }

    public void testRemoteIndexPathWithInvalidPathCreationMap() throws IOException {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new RemoteIndexPath(
                "djjsid73he8yd7usduh",
                2,
                new BlobPath().add("djsd878ndjh").add("hcs87cj8"),
                PathType.HASHED_PREFIX,
                PathHashAlgorithm.FNV_1A,
                new HashMap<>()
            )
        );
        assertEquals(
            "Invalid input in RemoteIndexPath constructor indexUUID=djjsid73he8yd7usduh shardCount=2 "
                + "basePath=[djsd878ndjh][hcs87cj8] pathType=HASHED_PREFIX pathHashAlgorithm=FNV_1A pathCreationMap={}",
            ex.getMessage()
        );
    }
}
