/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link ListDiscovery}, backed by a real {@link FsBlobStore} so that path separators behave as real
 * directories, the case that a plain slash-bearing {@code listBlobsByPrefix} would get wrong.
 *
 * <p>{@code ExtrasFS} is suppressed because it injects a random {@code extra0} entry into every created directory,
 * which a listing test sees as an object and which would also count against {@code max_objects} and
 * {@code max_list_requests}.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ListDiscoveryTests extends OpenSearchTestCase {

    private static final long UNLIMITED = Long.MAX_VALUE;

    private FsBlobStore store;
    private BlobContainer root;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Path dataDir = createTempDir().resolve("data");
        Files.createDirectories(dataDir);
        store = new FsBlobStore(8192, dataDir, false);
        root = store.blobContainer(BlobPath.cleanPath());
    }

    private static List<String> names(List<ObjectSlice> slices) {
        return slices.stream().map(ObjectSlice::getObjectName).toList();
    }

    private static long sizeOf(List<ObjectSlice> slices, String name) {
        return slices.stream().filter(s -> s.getObjectName().equals(name)).findFirst().orElseThrow().getEndOffsetExclusive();
    }

    private void write(String path, String content) throws Exception {
        String[] segments = path.split("/");
        BlobPath blobPath = BlobPath.cleanPath();
        for (int i = 0; i < segments.length - 1; i++) {
            blobPath = blobPath.add(segments[i]);
        }
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            store.blobContainer(blobPath).writeBlob(segments[segments.length - 1], in, bytes.length, true);
        }
    }

    public void testDiscoversOnlyTheOwnedSubPrefix() throws Exception {
        write("0/a.ndjson", "a1\n");
        write("0/b.ndjson", "b1\nb2\n");
        write("1/c.ndjson", "c1\n");

        List<ObjectSlice> shard0 = ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED);
        assertThat(names(shard0), contains("0/a.ndjson", "0/b.ndjson"));
        assertThat(sizeOf(shard0, "0/a.ndjson"), is(3L));
        assertThat(sizeOf(shard0, "0/b.ndjson"), is(6L));

        assertThat(names(ListDiscovery.discover(root, "1/", UNLIMITED, UNLIMITED)), contains("1/c.ndjson"));
    }

    public void testAbsentSubPrefixYieldsEmpty() throws Exception {
        write("0/a.ndjson", "a1\n");
        assertThat(ListDiscovery.discover(root, "7/", UNLIMITED, UNLIMITED).isEmpty(), is(true));
    }

    public void testEmptySubPrefixDirectoryYieldsEmpty() throws Exception {
        Files.createDirectories(store.path().resolve("3"));
        assertThat(ListDiscovery.discover(root, "3/", UNLIMITED, UNLIMITED).isEmpty(), is(true));
    }

    public void testBasePrefixSegmentsAreDescended() throws Exception {
        write("dump/0/a.ndjson", "a1\n");
        write("dump/1/b.ndjson", "b1\n");
        assertThat(names(ListDiscovery.discover(root, "dump/0/", UNLIMITED, UNLIMITED)), contains("dump/0/a.ndjson"));
    }

    public void testNestedDirectoriesBelowTheSubPrefixAreIncluded() throws Exception {
        write("0/top.ndjson", "t\n");
        write("0/nested/deep.ndjson", "d\n");
        write("0/nested/deeper/x.ndjson", "x\n");
        assertThat(
            names(ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED)),
            contains("0/nested/deep.ndjson", "0/nested/deeper/x.ndjson", "0/top.ndjson")
        );
    }

    public void testZeroByteObjectIsDiscoveredWithZeroSize() throws Exception {
        write("0/empty.ndjson", "");
        assertThat(sizeOf(ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED), "0/empty.ndjson"), is(0L));
    }

    public void testEmptySubPrefixListsTheRoot() throws Exception {
        write("a.ndjson", "a\n");
        assertThat(names(ListDiscovery.discover(root, "", UNLIMITED, UNLIMITED)), contains("a.ndjson"));
    }

    public void testResultIsLexicographicallyOrdered() throws Exception {
        write("0/z.ndjson", "z\n");
        write("0/a.ndjson", "a\n");
        write("0/m.ndjson", "m\n");
        assertThat(names(ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED)), contains("0/a.ndjson", "0/m.ndjson", "0/z.ndjson"));
    }

    public void testMaxObjectsExceededRejected() throws Exception {
        write("0/a.ndjson", "a\n");
        write("0/b.ndjson", "b\n");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ListDiscovery.discover(root, "0/", 1L, UNLIMITED));
        assertThat(e.getMessage(), containsString("found more than [max_objects=1] objects in total"));
        assertThat(e.getMessage(), containsString("the cap is cumulative"));
    }

    public void testMaxObjectsExactlyAtTheCapAllowed() throws Exception {
        write("0/a.ndjson", "a\n");
        write("0/b.ndjson", "b\n");
        assertThat(ListDiscovery.discover(root, "0/", 2L, UNLIMITED).size(), is(2));
    }

    public void testMaxObjectsIsCumulativeAcrossDirectories() throws Exception {
        write("0/a.ndjson", "a\n");
        write("0/b.ndjson", "b\n");
        write("0/sub/c.ndjson", "c\n");
        write("0/sub/d.ndjson", "d\n");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ListDiscovery.discover(root, "0/", 3L, UNLIMITED));
        assertThat(e.getMessage(), containsString("found more than [max_objects=3] objects in total under [0/]"));
        assertThat(e.getMessage(), containsString("was reached while listing [0/sub/]"));
    }

    public void testUnlimitedCapListsEveryObjectOfADirectoryLargerThanOneListPage() throws Exception {
        BlobContainer shardDirectory = store.blobContainer(BlobPath.cleanPath().add("0"));
        int count = ListDiscovery.KEYS_PER_LIST_PAGE + 1;
        byte[] bytes = "x\n".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < count; i++) {
            try (InputStream in = new ByteArrayInputStream(bytes)) {
                shardDirectory.writeBlob(String.format(Locale.ROOT, "f%05d.ndjson", i), in, bytes.length, true);
            }
        }

        List<ObjectSlice> slices = ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED);

        assertThat(slices.size(), is(count));
        assertThat(slices.get(count - 1).getObjectName(), is(String.format(Locale.ROOT, "0/f%05d.ndjson", count - 1)));
        assertThat(slices.get(count - 1).getEndOffsetExclusive(), is(2L));
    }

    public void testMaxListRequestsExceededRejected() throws Exception {
        write("0/a.ndjson", "a\n");
        CostLimitExceededException e = expectThrows(
            CostLimitExceededException.class,
            () -> ListDiscovery.discover(root, "0/", UNLIMITED, 1L)
        );
        assertThat(e.getMessage(), containsString("exceeds max_list_requests [1]"));
    }

    public void testGenerousMaxListRequestsAllowsTheWalk() throws Exception {
        write("0/a.ndjson", "a\n");
        assertThat(names(ListDiscovery.discover(root, "0/", UNLIMITED, 100L)), contains("0/a.ndjson"));
    }

    public void testNullContainerRejected() {
        expectThrows(NullPointerException.class, () -> ListDiscovery.discover(null, "0/", UNLIMITED, UNLIMITED));
    }

    public void testNullSubPrefixRejected() {
        expectThrows(NullPointerException.class, () -> ListDiscovery.discover(root, null, UNLIMITED, UNLIMITED));
    }

    public void testDepthAtTheCapIsAllowed() throws Exception {
        StringBuilder path = new StringBuilder("0");
        for (int i = 0; i < ListDiscovery.MAX_DEPTH; i++) {
            path.append("/d").append(i);
        }
        path.append("/deep.ndjson");
        write(path.toString(), "d\n");
        assertThat(ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED).size(), is(1));
    }

    public void testDepthBeyondTheCapRejected() throws Exception {
        StringBuilder path = new StringBuilder("0");
        for (int i = 0; i <= ListDiscovery.MAX_DEPTH; i++) {
            path.append("/d").append(i);
        }
        path.append("/deep.ndjson");
        write(path.toString(), "d\n");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED)
        );
        assertThat(e.getMessage(), containsString("exceeded the maximum directory depth [" + ListDiscovery.MAX_DEPTH + "]"));
    }

    public void testControlCharactersInAKeyAreDiscoveredVerbatim() throws Exception {
        write("0/we\rird\nname.ndjson", "x\n");
        List<ObjectSlice> discovered = ListDiscovery.discover(root, "0/", UNLIMITED, UNLIMITED);
        assertThat(names(discovered), contains("0/we\rird\nname.ndjson"));
        assertThat(LogSanitizer.sanitize(discovered.get(0).getObjectName()), is("0/we\\rird\\nname.ndjson"));
    }

    public void testListRequestsAreChargedPerBilledPageNotPerCall() throws Exception {
        int keys = ListDiscovery.KEYS_PER_LIST_PAGE + 1;
        for (int i = 0; i < keys; i++) {
            write("0/f" + i + ".ndjson", "x\n");
        }
        long perPage = 1 + 2 + 1;
        assertThat(ListDiscovery.discover(root, "0/", UNLIMITED, perPage).size(), is(keys));
        expectThrows(CostLimitExceededException.class, () -> ListDiscovery.discover(root, "0/", UNLIMITED, perPage - 1));
    }

    public void testPageSizeMatchesTheS3ListMaximum() {
        assertThat(ListDiscovery.KEYS_PER_LIST_PAGE, is(1000));
    }

    public void testSubPrefixWithoutTrailingSlashRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ListDiscovery.discover(root, "0", UNLIMITED, UNLIMITED)
        );
        assertThat(e.getMessage(), containsString("must be empty or end with [/]"));
    }

    public void testSubPrefixWithEmptySegmentRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ListDiscovery.discover(root, "a//0/", UNLIMITED, UNLIMITED)
        );
        assertThat(e.getMessage(), containsString("empty path segment"));
    }

    public void testSubPrefixWithLeadingSlashRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ListDiscovery.discover(root, "/0/", UNLIMITED, UNLIMITED)
        );
        assertThat(e.getMessage(), containsString("leading [/]"));
    }
}
