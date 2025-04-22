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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common;

import org.opensearch.action.pagination.PageToken;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.common.Strings;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

/**
 * A table.
 *
 * @opensearch.internal
 */
public class Table {

    private List<Cell> headers = new ArrayList<>();
    private List<List<Cell>> rows = new ArrayList<>();
    private Map<String, List<Cell>> map = new HashMap<>();
    private Map<String, Cell> headerMap = new HashMap<>();
    private List<Cell> currentCells;
    private boolean inHeaders = false;
    private boolean withTime = false;
    /**
     * paginatedQueryResponse if null will imply the Table response is not paginated.
     */
    private PageToken pageToken;
    public static final String EPOCH = "epoch";
    public static final String TIMESTAMP = "timestamp";

    public Table() {}

    public Table(@Nullable PageToken pageToken) {
        this.pageToken = pageToken;
    }

    public Table startHeaders() {
        inHeaders = true;
        currentCells = new ArrayList<>();
        return this;
    }

    public Table startHeadersWithTimestamp() {
        startHeaders();
        this.withTime = true;
        addCell("epoch", "alias:t,time;desc:seconds since 1970-01-01 00:00:00");
        addCell("timestamp", "alias:ts,hms,hhmmss;desc:time in HH:MM:SS");
        return this;
    }

    public Table endHeaders() {
        if (currentCells == null || currentCells.isEmpty()) {
            throw new IllegalStateException("no headers added...");
        }
        inHeaders = false;
        headers = currentCells;
        currentCells = null;

        /* Create associative structure for columns that
         * contain the same cells as the rows:
         *
         *     header1 => [Cell, Cell, ...]
         *     header2 => [Cell, Cell, ...]
         *     header3 => [Cell, Cell, ...]
         *
         * Also populate map to look up headers by name.
         *
         */
        for (Cell header : headers) {
            map.put(header.value.toString(), new ArrayList<Cell>());
            headerMap.put(header.value.toString(), header);
        }

        return this;
    }

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("HH:mm:ss").withZone(ZoneOffset.UTC);

    public Table startRow() {
        if (headers.isEmpty()) {
            throw new IllegalStateException("no headers added...");
        }
        currentCells = new ArrayList<>(headers.size());
        if (withTime) {
            long time = System.currentTimeMillis();
            addCell(TimeUnit.SECONDS.convert(time, TimeUnit.MILLISECONDS));
            addCell(FORMATTER.format(Instant.ofEpochMilli(time)));
        }
        return this;
    }

    public Table endRow(boolean check) {
        if (currentCells == null) {
            throw new IllegalStateException("no row started...");
        }
        if (check && (currentCells.size() != headers.size())) {
            StringBuilder s = new StringBuilder();
            s.append("mismatch on number of cells ");
            s.append(currentCells.size());
            s.append(" in a row compared to header ");
            s.append(headers.size());
            throw new IllegalStateException(s.toString());
        }
        rows.add(currentCells);
        currentCells = null;
        return this;
    }

    public Table endRow() {
        endRow(true);
        return this;
    }

    public Table addCell(Object value) {
        return addCell(value, "");
    }

    public Table addCell(Object value, String attributes) {
        if (currentCells == null) {
            throw new IllegalStateException("no block started...");
        }
        if (!inHeaders) {
            if (currentCells.size() == headers.size()) {
                throw new IllegalStateException("can't add more cells to a row than the header");
            }
        }
        Map<String, String> mAttr;
        if (attributes.length() == 0) {
            if (inHeaders) {
                mAttr = emptyMap();
            } else {
                // get the attributes of the header cell we are going to add to
                mAttr = headers.get(currentCells.size()).attr;
            }
        } else {
            mAttr = new HashMap<>();
            if (!inHeaders) {
                // get the attributes of the header cell we are going to add
                mAttr.putAll(headers.get(currentCells.size()).attr);
            }
            String[] sAttrs = attributes.split(";");
            for (String sAttr : sAttrs) {
                if (sAttr.length() == 0) {
                    continue;
                }
                int idx = sAttr.indexOf(':');
                mAttr.put(sAttr.substring(0, idx), sAttr.substring(idx + 1));
            }
        }

        Cell cell = new Cell(value, mAttr);
        int cellIndex = currentCells.size();
        currentCells.add(cell);

        // If we're in a value row, also populate the named column.
        if (!inHeaders) {
            String hdr = (String) headers.get(cellIndex).value;
            map.get(hdr).add(cell);
        }

        return this;
    }

    public List<Cell> getHeaders() {
        return this.headers;
    }

    public List<List<Cell>> getRows() {
        return rows;
    }

    public Map<String, List<Cell>> getAsMap() {
        return this.map;
    }

    public Map<String, Cell> getHeaderMap() {
        return this.headerMap;
    }

    public Cell findHeaderByName(String header) {
        for (Cell cell : headers) {
            if (cell.value.toString().equals(header)) {
                return cell;
            }
        }
        return null;
    }

    public Map<String, String> getAliasMap() {
        Map<String, String> headerAliasMap = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            Cell headerCell = headers.get(i);
            String headerName = headerCell.value.toString();
            if (headerCell.attr.containsKey("alias")) {
                String[] aliases = Strings.splitStringByCommaToArray(headerCell.attr.get("alias"));
                for (String alias : aliases) {
                    headerAliasMap.put(alias, headerName);
                }
            }
            headerAliasMap.put(headerName, headerName);
        }
        return headerAliasMap;
    }

    public PageToken getPageToken() {
        return pageToken;
    }

    /**
     * Cell in a table
     *
     * @opensearch.internal
     */
    public static class Cell {
        public final Object value;
        public final Map<String, String> attr;

        public Cell(Object value, Cell other) {
            this.value = value;
            this.attr = other.attr;
        }

        public Cell(Object value) {
            this.value = value;
            this.attr = new HashMap<>();
        }

        public Cell(Object value, Map<String, String> attr) {
            this.value = value;
            this.attr = attr;
        }
    }
}
