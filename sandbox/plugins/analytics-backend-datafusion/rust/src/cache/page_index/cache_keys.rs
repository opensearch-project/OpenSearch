/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cache key types for the two scoped page-index caches.

use std::fmt::Display;
use std::sync::Arc;

use parquet::file::page_index::offset_index::OffsetIndexMetaData;

/// ColumnIndex cache key — one decoded `ColumnIndexMetaData` **cell** per
/// `(file, column, row-group)`. The page index for a given column+RG is an
/// intrinsic property of the file: it is identical no matter which *other*
/// columns a query filters on, or which literal a predicate uses. Keying at the
/// cell granularity means a column's per-page string min/max is decoded and
/// stored **once per file**, then reused by every query whose predicate touches
/// that column — regardless of the predicate-column *combination* or the
/// surviving-row-group *set*. (The prior set-keyed design re-decoded and
/// re-stored a column for every distinct predicate/RG combination — storage grew
/// with query diversity, not schema width.)
///
/// Both scan paths resolve the same `(file, col, rg)` for the same logical
/// request, so cells are shared across paths → cross-path sharing.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct CiCellKey {
    pub(crate) path: Arc<str>,
    pub(crate) col: usize,
    pub(crate) rg: usize,
}

impl Display for CiCellKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.path, self.col, self.rg)
    }
}

/// OffsetIndex cache key — one decoded value per `(file, column)`, where the
/// value is that column's `OffsetIndexMetaData` for **every** row group (a
/// `Vec` indexed by RG). Unlike the ColumnIndex, the OffsetIndex is read at scan
/// time for any RG DataFusion chooses to scan — and DataFusion picks that set
/// itself, after our load — so a column's OffsetIndex must always cover all RGs
/// (an empty entry on a scanned RG panics / breaks reads). RG can therefore never
/// be a key axis here; the cell is the whole-column, all-RG offset index. Keyed
/// only on `(file, col)`, so any query that reads a column reuses its offset
/// index irrespective of projection or predicate.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct OiCellKey {
    pub(crate) path: Arc<str>,
    pub(crate) col: usize,
}

impl Display for OiCellKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.path, self.col)
    }
}

/// One column's OffsetIndex across all row groups (indexed by RG). The value type
/// of [`OFFSET_INDEX_CACHE`].
pub(crate) type OiColumn = Vec<OffsetIndexMetaData>;
