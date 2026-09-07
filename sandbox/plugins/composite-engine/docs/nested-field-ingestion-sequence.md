# Nested field ingestion — sequence diagram

Documents the full path a document with a `nested` field takes on a composite
(Parquet primary + Lucene secondary) index, from the write call through writer
selection, document parsing, and the per-format write paths. Every behavior
shown here — including the decision and the gap called out at the bottom —
has been verified against a running node, not just read from source.

## Sequence diagram

```mermaid
sequenceDiagram
    participant C as Client (index request)
    participant E as DataFormatAwareEngine
    participant WP as WriterPool
    participant W as ParquetWriter (active generation)
    participant VM as VSRManager
    participant DP as DocumentParser
    participant CDI as CompositeDocumentInput
    participant PDI as ParquetDocumentInput
    participant LDI as LuceneDocumentInput

    C->>E: index(doc)
    E->>E: mappingVersion = currentMappingVersion()

    Note over E,WP: Writer selection — a VERSION NUMBER check only.<br/>No field names/types/nesting are inspected here.
    E->>WP: getAndLock(w => w.isSchemaMutable() || w.mappingVersion() >= mappingVersion)
    alt active writer's schema still mutable (never flushed to disk), or already current
        WP-->>E: same writer (reused)
        E->>W: updateMappingVersion(mappingVersion)
        Note over W,VM: reconcileSchema compares TOP-LEVEL field names only.<br/>A new top-level field is patched in, but a new leaf inside an<br/>EXISTING nested struct's children is silently NOT patched.
        W->>VM: reconcileSchema(freshSchema)
    else writer already initialized (flushed) AND stale
        WP-->>E: brand-new writer, fresh schema built from scratch
    end

    E->>DP: parse(doc)
    loop for each array element under a nested field
        DP->>CDI: startNestedChild(path)
        CDI->>PDI: startNestedChild(path)
        CDI->>LDI: startNestedChild(path)
        loop for each leaf in this element
            alt leaf is declared in the mapping
                DP->>CDI: addField(fieldType, value)
                CDI->>PDI: addField(fieldType, value)
                Note over PDI: buffered into an in-memory NestedChild tree<br/>(real per-element structure preserved)
                CDI->>LDI: addField(fieldType, value)
                Note over LDI: no-op — nested leaves are NEVER represented<br/>in Lucene at all, regardless of type. Parquet-only, by design.
            else leaf is undeclared (dynamic field)
                alt nested field sets dynamic:false
                    DP->>DP: parseDynamicValue — early return
                    Note over DP: value discarded before reaching<br/>ANY DocumentInput — silent, permanent, consistent
                else nested field sets dynamic:strict
                    DP->>C: throw strict_dynamic_mapping_exception (400)
                    Note over DP,C: whole document rejected — verified live:<br/>no partial row, no leaked data, no seq_no consumed
                end
            end
        end
        DP->>CDI: endNestedChild()
        CDI->>PDI: endNestedChild()
        CDI->>LDI: endNestedChild()
    end

    E->>W: addDoc(documentInput)
    W->>VM: addDoc(documentInput)
    VM->>VM: writeNestedChildren → writeChildList
    loop for each buffered leaf
        VM->>VM: structVector.getChild(leafName)
        alt child vector exists
            VM->>VM: setLeafValue(vector, value) — real typed Arrow write
        else child vector missing (reconcile gap)
            VM->>VM: WARN "has no child vector [x] — skipping"
            Note over VM: value silently dropped from Parquet too
        end
    end
    VM-->>W: WriteResult.Success
    W-->>E: WriteResult.Success
    E-->>C: 201 Created
```

## Decisions and gaps this diagram reflects

1. **Nested and `flat_object` data is Parquet-only — Lucene represents none of
   it, by decision, not by limitation.** Earlier, Lucene flattened keyword/text
   nested leaves into a shared doc-values field and skipped other types; that
   was superseded — Lucene now does nothing at all for anything inside a
   nested scope, or for any `flat_object` entry (root or nested). This
   eliminates the correlation-loss and type-loss concerns that flattening had,
   at the cost of Lucene being unable to answer even a single-leaf
   existence/term query for nested data — every nested/`flat_object` query
   must go through Parquet/DataFusion.
2. **Schema reconciliation is top-level-name-only** (`VSRManager.reconcileSchema`).
   A new leaf added to an *already-active* nested field's `properties` (via a
   mapping update) is not reliably patched into the running writer's struct —
   reproduced live in 4 of 5 runs (writer not yet flushed → reused → silently
   dropped from Parquet; writer already flushed → new generation → safe). Not
   introduced by the nested-field work; pre-existing in
   `VSRManager.reconcileSchema`/`DataFormatAwareEngine`'s writer-selection
   predicate. Since Lucene no longer carries any nested data at all, this gap
   no longer manifests as *divergence between formats* — but it is not
   thereby safer: Parquet is now the only copy of nested data, so when this
   gap drops a value, it is lost everywhere, with no secondary copy to fall
   back on. This gap is unaffected by decision 1 and still needs its own fix.

`dynamic:false` and `dynamic:strict` are both confirmed safe against silent
cross-format divergence for *undeclared* fields; neither protects against
gap 2, since that's triggered by an explicit, declared mapping change, not
dynamic field discovery.
