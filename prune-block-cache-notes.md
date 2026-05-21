# Prune Block Cache API — Implementation Notes

## Branch
- **Working branch:** `feature/prune-block-cache-main`
- **Base:** `origin/main` (opensearch-project/OpenSearch)
- **Commit:** `c0a0cb4df4b` — "Add /_blockcache/prune API for block cache (Foyer)"

## API

```
POST /_blockcache/prune?cache=disk
POST /_blockcache/prune?cache=disk&nodes=node1,node2   # target specific warm nodes
POST /_blockcache/prune?cache=disk&timeout=30s
```

**`cache` param** — required. Only valid value currently: `disk` (Foyer).  
Omitting or passing an unknown value returns 400.

**Sample response:**
```json
{
  "acknowledged": true,
  "summary": {
    "total_nodes_targeted": 2,
    "successful_nodes": 2,
    "failed_nodes": 0
  },
  "nodes": {
    "warm-node-1": { "name": "...", "cleared": true },
    "warm-node-2": { "name": "...", "cleared": true }
  }
}
```

## Permission
Action name: `cluster:admin/blockcache/prune` → requires `cluster_manage` privilege.

## How `cache=disk` maps to Foyer
- `BlockCacheConstants.DISK_CACHE = "disk"` — [code link](https://github.com/vishwasgarg18/OpenSearch/blob/feature/warm-cache-spi-and-stats/server/src/main/java/org/opensearch/plugins/BlockCacheConstants.java#L31)
- `FoyerBlockCache.cacheName()` returns `BlockCacheConstants.DISK_CACHE` — [code link](https://github.com/vishwasgarg18/OpenSearch/blob/feature/warm-cache-spi-and-stats/sandbox/plugins/block-cache-foyer/src/main/java/org/opensearch/blockcache/foyer/FoyerBlockCache.java#L68-L70)
- `NodeCacheOrchestrator.get(name)` matches on `cacheName()` — registry lookup

## Files Changed

### Rust (Foyer native)
| File | Change |
|------|--------|
| `sandbox/plugins/block-cache-foyer/src/main/rust/src/foyer/foyer_cache.rs` | Added `clear_sync()` — blocks on async `clear()` using owned Tokio runtime |
| `sandbox/plugins/block-cache-foyer/src/main/rust/src/foyer/ffm.rs` | Added `foyer_clear_cache(ptr)` C-ABI export |

### Java (Foyer plugin)
| File | Change |
|------|--------|
| `sandbox/plugins/block-cache-foyer/src/main/java/org/opensearch/blockcache/foyer/FoyerBridge.java` | Added `FOYER_CLEAR_CACHE` FFM handle + `clearCache(ptr)` method |
| `sandbox/plugins/block-cache-foyer/src/main/java/org/opensearch/blockcache/foyer/FoyerBlockCache.java` | Added `clear()` override calling `FoyerBridge.clearCache()` |

### Java (Core SPI)
| File | Change |
|------|--------|
| `server/src/main/java/org/opensearch/plugins/BlockCache.java` | Added `default void clear() {}` — future backends get a no-op |

### Java (Transport/REST)
| File | Change |
|------|--------|
| `server/src/main/java/org/opensearch/action/admin/cluster/blockcache/PruneBlockCacheAction.java` | Action type, name = `cluster:admin/blockcache/prune` |
| `server/src/main/java/org/opensearch/action/admin/cluster/blockcache/PruneBlockCacheRequest.java` | Carries `cacheName` + optional node IDs |
| `server/src/main/java/org/opensearch/action/admin/cluster/blockcache/NodePruneBlockCacheResponse.java` | Per-node `cleared: true/false` |
| `server/src/main/java/org/opensearch/action/admin/cluster/blockcache/PruneBlockCacheResponse.java` | Cluster-wide aggregation |
| `server/src/main/java/org/opensearch/action/admin/cluster/blockcache/TransportPruneBlockCacheAction.java` | Resolves to warm nodes, looks up cache by name, calls `cache.clear()` |
| `server/src/main/java/org/opensearch/rest/action/admin/cluster/RestPruneBlockCacheAction.java` | REST handler, validates `cache` param |
| `server/src/main/java/org/opensearch/action/ActionModule.java` | Registered transport action + REST handler |
| `server/src/main/java/org/opensearch/node/Node.java` | Bound `BlockCacheRegistry` → `nodeCacheOrchestrator` in Guice |

### Tests
| File | Tests |
|------|-------|
| `server/src/test/java/org/opensearch/action/admin/cluster/blockcache/TransportPruneBlockCacheActionTests.java` | nodeOperation, nullRegistry, warmNodeResolution, specificNodeTargeting, nonWarmNodeThrows, responseAggregation (x2) |
| `server/src/test/java/org/opensearch/rest/action/admin/cluster/RestPruneBlockCacheActionTests.java` | routes, getName, validCacheParam, missingCacheParam, unknownCacheParam, circuitBreaker, nodeTargeting |

## Plugin Initialization Behaviour
| Scenario | Behaviour |
|----------|-----------|
| Non-warm node (no orchestrator) | `BlockCacheRegistry` injected as `null` → `cleared=false`, no error |
| Warm node, Foyer plugin not loaded | `registry.get("disk")` returns empty → 400 `No block cache registered with name [disk]` |
| Warm node, Foyer plugin loaded | `cache.clear()` called → `cleared=true` |

## Design Decisions
- **`cache=disk` not `cache=foyer`** — name is role-based (`BlockCacheConstants.DISK_CACHE`), not library-based. Stable if backend changes.
- **Generic SPI** — `BlockCache.clear()` is a default no-op; any future backend just overrides it.
- **Targets warm nodes only** — consistent with `/_filecache/prune` pattern.
- **`?cache=` required** — omit = 400, unknown = 400 (consistent with `_nodes/stats/{metric}` pattern).
