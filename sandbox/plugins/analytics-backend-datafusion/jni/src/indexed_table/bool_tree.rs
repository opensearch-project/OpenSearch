/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Boolean query tree representation and deserialization.

Wire format (matches Java `IndexFilterTreeNode.serialize()`):
  [tag: u8] [payload...]
  tag 0 = And:       [child_count: u16_le] [child_0] [child_1] ...
  tag 1 = Or:        [child_count: u16_le] [child_0] [child_1] ...
  tag 2 = Not:       [child]
  tag 3 = Collector: [provider_id: u16_le] [collector_idx: u16_le]
  tag 4 = Predicate: [predicate_id: u16_le]

Collectors are passed separately — `collector_idx` indexes into that array.
`provider_id` identifies which collector engine handles this leaf.

Predicate leaves carry only an ID. The actual column/op/value is resolved
from the Substrait plan during tree resolution.
**/

use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;

use super::index::RowGroupDocsCollector;

/// A node in the boolean query tree (unresolved).
#[derive(Debug, Clone)]
pub enum BoolNode {
    And(Vec<BoolNode>),
    Or(Vec<BoolNode>),
    Not(Box<BoolNode>),
    Collector { provider_id: u16, collector_idx: usize },
    Predicate { predicate_id: u16 },
}

/// Resolved tree with collectors and predicates attached (ready for evaluation).
#[derive(Debug)]
pub enum ResolvedNode {
    And(Vec<ResolvedNode>),
    Or(Vec<ResolvedNode>),
    Not(Box<ResolvedNode>),
    Collector { provider_id: u16, collector: Arc<dyn RowGroupDocsCollector> },
    Predicate { column: String, op: Operator, value: ScalarValue },
}

/// A resolved predicate extracted from the Substrait plan.
#[derive(Debug, Clone)]
pub struct ResolvedPredicate {
    pub column: String,
    pub op: Operator,
    pub value: ScalarValue,
}

impl BoolNode {
    /// Count the number of Collector leaves in the tree.
    pub fn collector_leaf_count(&self) -> usize {
        match self {
            BoolNode::And(children) | BoolNode::Or(children) => {
                children.iter().map(|c| c.collector_leaf_count()).sum()
            }
            BoolNode::Not(child) => child.collector_leaf_count(),
            BoolNode::Collector { .. } => 1,
            BoolNode::Predicate { .. } => 0,
        }
    }

    /// Collect all unique (provider_id, collector_idx) pairs from the tree.
    pub fn collector_leaves(&self) -> Vec<(u16, usize)> {
        let mut leaves = Vec::new();
        self.collect_leaves(&mut leaves);
        leaves
    }

    fn collect_leaves(&self, out: &mut Vec<(u16, usize)>) {
        match self {
            BoolNode::And(children) | BoolNode::Or(children) => {
                for c in children { c.collect_leaves(out); }
            }
            BoolNode::Not(child) => child.collect_leaves(out),
            BoolNode::Collector { provider_id, collector_idx } => {
                out.push((*provider_id, *collector_idx));
            }
            BoolNode::Predicate { .. } => {}
        }
    }

    /// De Morgan's NOT push-down normalization.
    /// After this, NOT only appears directly above Collector or Predicate leaves.
    pub fn push_not_down(self) -> BoolNode {
        match self {
            BoolNode::And(children) => {
                BoolNode::And(children.into_iter().map(|c| c.push_not_down()).collect())
            }
            BoolNode::Or(children) => {
                BoolNode::Or(children.into_iter().map(|c| c.push_not_down()).collect())
            }
            BoolNode::Not(child) => push_not_into(*child),
            leaf => leaf,
        }
    }

    /// Resolve collector indices and predicate IDs into concrete references.
    pub fn resolve(
        &self,
        collectors: &[Arc<dyn RowGroupDocsCollector>],
        predicates: &[ResolvedPredicate],
    ) -> Result<ResolvedNode, String> {
        match self {
            BoolNode::And(children) => {
                let resolved: Result<Vec<_>, _> =
                    children.iter().map(|c| c.resolve(collectors, predicates)).collect();
                Ok(ResolvedNode::And(resolved?))
            }
            BoolNode::Or(children) => {
                let resolved: Result<Vec<_>, _> =
                    children.iter().map(|c| c.resolve(collectors, predicates)).collect();
                Ok(ResolvedNode::Or(resolved?))
            }
            BoolNode::Not(child) => {
                Ok(ResolvedNode::Not(Box::new(child.resolve(collectors, predicates)?)))
            }
            BoolNode::Collector { provider_id, collector_idx } => {
                let collector = collectors
                    .get(*collector_idx)
                    .ok_or_else(|| format!("collector_idx {} out of range", collector_idx))?;
                Ok(ResolvedNode::Collector { provider_id: *provider_id, collector: Arc::clone(collector) })
            }
            BoolNode::Predicate { predicate_id } => {
                let pred = predicates
                    .get(*predicate_id as usize)
                    .ok_or_else(|| format!("predicate_id {} out of range", predicate_id))?;
                Ok(ResolvedNode::Predicate {
                    column: pred.column.clone(),
                    op: pred.op,
                    value: pred.value.clone(),
                })
            }
        }
    }
}

// ── De Morgan's NOT push-down ──────────────────────────────────────────

fn push_not_into(child: BoolNode) -> BoolNode {
    match child {
        BoolNode::And(children) => {
            BoolNode::Or(children.into_iter().map(push_not_into).collect())
                .push_not_down()
        }
        BoolNode::Or(children) => {
            BoolNode::And(children.into_iter().map(push_not_into).collect())
                .push_not_down()
        }
        BoolNode::Not(inner) => inner.push_not_down(),
        // Predicate and Collector under NOT stay wrapped — evaluator handles negation
        leaf => BoolNode::Not(Box::new(leaf)),
    }
}

// ── Deserialization ────────────────────────────────────────────────────

const TAG_AND: u8 = 0;
const TAG_OR: u8 = 1;
const TAG_NOT: u8 = 2;
const TAG_COLLECTOR: u8 = 3;
const TAG_PREDICATE: u8 = 4;

impl BoolNode {
    /// Deserialize from bytes (Java wire format).
    pub fn deserialize(data: &[u8]) -> Result<BoolNode, String> {
        let mut cursor = 0;
        Self::read_from(data, &mut cursor)
    }

    fn read_from(data: &[u8], cursor: &mut usize) -> Result<BoolNode, String> {
        if *cursor >= data.len() {
            return Err("unexpected end of data".into());
        }
        let tag = data[*cursor];
        *cursor += 1;

        match tag {
            TAG_AND | TAG_OR => {
                let count = read_u16(data, cursor)? as usize;
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(Self::read_from(data, cursor)?);
                }
                Ok(if tag == TAG_AND { BoolNode::And(children) } else { BoolNode::Or(children) })
            }
            TAG_NOT => Ok(BoolNode::Not(Box::new(Self::read_from(data, cursor)?))),
            TAG_COLLECTOR => {
                let provider_id = read_u16(data, cursor)?;
                let collector_idx = read_u16(data, cursor)? as usize;
                Ok(BoolNode::Collector { provider_id, collector_idx })
            }
            TAG_PREDICATE => {
                let predicate_id = read_u16(data, cursor)?;
                Ok(BoolNode::Predicate { predicate_id })
            }
            _ => Err(format!("unknown tag: {}", tag)),
        }
    }
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Result<u16, String> {
    if *cursor + 2 > data.len() { return Err("unexpected end reading u16".into()); }
    let v = u16::from_le_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2; Ok(v)
}

// ══════════════════════════════════════════════════════════════════════
// Substrait-driven tree types (new approach — coexists with old BoolNode)
// ══════════════════════════════════════════════════════════════════════

/// BoolNode for the Substrait-driven approach.
/// Collector carries column+value strings instead of integer indices.
#[derive(Debug, Clone)]
pub enum SubstraitBoolNode {
    And(Vec<SubstraitBoolNode>),
    Or(Vec<SubstraitBoolNode>),
    Not(Box<SubstraitBoolNode>),
    Collector { column: String, value: String },
    Predicate { predicate_id: u16 },
}

/// Partially resolved tree: predicates resolved upfront, collectors carry
/// column+value for lazy per-segment resolution via JNI callbacks.
#[derive(Debug)]
pub enum PartiallyResolvedNode {
    And(Vec<PartiallyResolvedNode>),
    Or(Vec<PartiallyResolvedNode>),
    Not(Box<PartiallyResolvedNode>),
    Collector { column: String, value: String },
    Predicate { column: String, op: Operator, value: ScalarValue },
}

impl SubstraitBoolNode {
    /// De Morgan's NOT push-down normalization.
    /// After this, NOT only appears directly above Collector or Predicate leaves.
    pub fn push_not_down(self) -> SubstraitBoolNode {
        match self {
            SubstraitBoolNode::And(children) => {
                SubstraitBoolNode::And(children.into_iter().map(|c| c.push_not_down()).collect())
            }
            SubstraitBoolNode::Or(children) => {
                SubstraitBoolNode::Or(children.into_iter().map(|c| c.push_not_down()).collect())
            }
            SubstraitBoolNode::Not(child) => substrait_push_not_into(*child),
            leaf => leaf,
        }
    }

    /// Resolve Predicate nodes using the predicates vec, leaving Collector nodes as-is.
    pub fn partially_resolve(
        &self,
        predicates: &[ResolvedPredicate],
    ) -> Result<PartiallyResolvedNode, String> {
        match self {
            SubstraitBoolNode::And(children) => {
                let resolved: Result<Vec<_>, _> =
                    children.iter().map(|c| c.partially_resolve(predicates)).collect();
                Ok(PartiallyResolvedNode::And(resolved?))
            }
            SubstraitBoolNode::Or(children) => {
                let resolved: Result<Vec<_>, _> =
                    children.iter().map(|c| c.partially_resolve(predicates)).collect();
                Ok(PartiallyResolvedNode::Or(resolved?))
            }
            SubstraitBoolNode::Not(child) => {
                Ok(PartiallyResolvedNode::Not(Box::new(child.partially_resolve(predicates)?)))
            }
            SubstraitBoolNode::Collector { column, value } => {
                Ok(PartiallyResolvedNode::Collector {
                    column: column.clone(),
                    value: value.clone(),
                })
            }
            SubstraitBoolNode::Predicate { predicate_id } => {
                let pred = predicates
                    .get(*predicate_id as usize)
                    .ok_or_else(|| format!("predicate_id {} out of range", predicate_id))?;
                Ok(PartiallyResolvedNode::Predicate {
                    column: pred.column.clone(),
                    op: pred.op,
                    value: pred.value.clone(),
                })
            }
        }
    }
}

/// De Morgan's NOT push-down for SubstraitBoolNode.
fn substrait_push_not_into(child: SubstraitBoolNode) -> SubstraitBoolNode {
    match child {
        SubstraitBoolNode::And(children) => {
            SubstraitBoolNode::Or(children.into_iter().map(substrait_push_not_into).collect())
                .push_not_down()
        }
        SubstraitBoolNode::Or(children) => {
            SubstraitBoolNode::And(children.into_iter().map(substrait_push_not_into).collect())
                .push_not_down()
        }
        SubstraitBoolNode::Not(inner) => inner.push_not_down(),
        leaf => SubstraitBoolNode::Not(Box::new(leaf)),
    }
}

impl PartiallyResolvedNode {
    /// Resolve Collector nodes to the existing ResolvedNode that tree_eval.rs expects.
    /// The `collectors` vec is indexed by the order Collector nodes appear in the tree
    /// (left-to-right DFS traversal order).
    pub fn resolve_collectors(
        &self,
        collectors: &[Arc<dyn RowGroupDocsCollector>],
    ) -> Result<ResolvedNode, String> {
        let mut idx = 0;
        self.resolve_collectors_inner(collectors, &mut idx)
    }

    fn resolve_collectors_inner(
        &self,
        collectors: &[Arc<dyn RowGroupDocsCollector>],
        idx: &mut usize,
    ) -> Result<ResolvedNode, String> {
        match self {
            PartiallyResolvedNode::And(children) => {
                let resolved: Result<Vec<_>, _> =
                    children.iter().map(|c| c.resolve_collectors_inner(collectors, idx)).collect();
                Ok(ResolvedNode::And(resolved?))
            }
            PartiallyResolvedNode::Or(children) => {
                let resolved: Result<Vec<_>, _> =
                    children.iter().map(|c| c.resolve_collectors_inner(collectors, idx)).collect();
                Ok(ResolvedNode::Or(resolved?))
            }
            PartiallyResolvedNode::Not(child) => {
                Ok(ResolvedNode::Not(Box::new(child.resolve_collectors_inner(collectors, idx)?)))
            }
            PartiallyResolvedNode::Collector { .. } => {
                let collector = collectors
                    .get(*idx)
                    .ok_or_else(|| format!("collector index {} out of range (have {})", *idx, collectors.len()))?;
                let node = ResolvedNode::Collector {
                    provider_id: 0, // not used in Substrait-driven path
                    collector: Arc::clone(collector),
                };
                *idx += 1;
                Ok(node)
            }
            PartiallyResolvedNode::Predicate { column, op, value } => {
                Ok(ResolvedNode::Predicate {
                    column: column.clone(),
                    op: *op,
                    value: value.clone(),
                })
            }
        }
    }

    /// Count the number of Collector leaves in the tree (left-to-right DFS order).
    pub fn collector_leaf_count(&self) -> usize {
        match self {
            PartiallyResolvedNode::And(children) | PartiallyResolvedNode::Or(children) => {
                children.iter().map(|c| c.collector_leaf_count()).sum()
            }
            PartiallyResolvedNode::Not(child) => child.collector_leaf_count(),
            PartiallyResolvedNode::Collector { .. } => 1,
            PartiallyResolvedNode::Predicate { .. } => 0,
        }
    }

    /// Collect all (column, value) pairs from Collector leaves in DFS order.
    pub fn collector_leaves(&self) -> Vec<(String, String)> {
        let mut leaves = Vec::new();
        self.collect_collector_leaves(&mut leaves);
        leaves
    }

    fn collect_collector_leaves(&self, out: &mut Vec<(String, String)>) {
        match self {
            PartiallyResolvedNode::And(children) | PartiallyResolvedNode::Or(children) => {
                for c in children { c.collect_collector_leaves(out); }
            }
            PartiallyResolvedNode::Not(child) => child.collect_collector_leaves(out),
            PartiallyResolvedNode::Collector { column, value } => {
                out.push((column.clone(), value.clone()));
            }
            PartiallyResolvedNode::Predicate { .. } => {}
        }
    }
}
