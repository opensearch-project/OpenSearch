/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FileInfos}.
 */
public class FileInfosTests extends OpenSearchTestCase {

    private DataFormat testFormat() {
        DataFormat format = mock(DataFormat.class);
        when(format.name()).thenReturn("test");
        return format;
    }

    public void testEmptyFileInfos() {
        FileInfos empty = FileInfos.empty();
        assertTrue(empty.writerFilesMap().isEmpty());
        assertNull(empty.rowIdMapping());
    }

    public void testBuilderWithoutRowIdMapping() {
        DataFormat format = testFormat();
        WriterFileSet wfs = WriterFileSet.builder()
            .directory(Path.of("/tmp"))
            .writerGeneration(0)
            .addNumRows(10)
            .addFile("file1.dat")
            .build();

        FileInfos infos = FileInfos.builder().putWriterFileSet(format, wfs).build();

        assertNotNull(infos.writerFilesMap());
        assertEquals(1, infos.writerFilesMap().size());
        assertNull(infos.rowIdMapping());
        assertTrue(infos.getWriterFileSet(format).isPresent());
    }

    public void testBuilderWithRowIdMapping() {
        DataFormat format = testFormat();
        WriterFileSet wfs = WriterFileSet.builder()
            .directory(Path.of("/tmp"))
            .writerGeneration(0)
            .addNumRows(3)
            .addFile("file1.dat")
            .build();

        RowIdMapping mapping = new PackedRowIdMapping(new long[] { 2, 0, 1 }, true);

        FileInfos infos = FileInfos.builder().putWriterFileSet(format, wfs).rowIdMapping(mapping).build();

        assertNotNull(infos.rowIdMapping());
        assertEquals(3, infos.rowIdMapping().size());
        assertEquals(2L, infos.rowIdMapping().getNewRowId(0));
    }

    public void testConstructorWithoutMapping() {
        FileInfos infos = new FileInfos(java.util.Map.of());
        assertNull(infos.rowIdMapping());
        assertTrue(infos.writerFilesMap().isEmpty());
    }

    public void testGetWriterFileSetMissing() {
        FileInfos infos = FileInfos.empty();
        DataFormat format = testFormat();
        assertFalse(infos.getWriterFileSet(format).isPresent());
    }
}
