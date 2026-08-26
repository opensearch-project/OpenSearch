# Remote Store Fence — formal specification

TLA+ models of the object-store-backed primary fencing protocol
(`RemoteStoreFence.java`, PR #22774 / RFC #22768). One mutable
`fence__<term>` object per primary term, written only by compare-and-swap.
The fencing token is the CAS chain — the object's version token, or ETag —
and not the primary term itself.

## Terms

Everything below leans on these, so they are worth pinning down first.

| Term | What it means |
|---|---|
| **control flow** | The path carrying decisions about *who may write*. The fence object is the only thing on it. |
| **translog flow** | Translog files and their metadata — the acknowledgement path, gated directly by the fence CAS. |
| **segment flow** | Segment files and their metadata — what a recovering copy hydrates from, gated by an ownership check rather than a CAS. |
| **fence** | The small mutable object that records who currently holds the right to acknowledge writes. |
| **CAS** | Compare-and-swap: a write the store accepts only if the object's current version is the one you present, and refuses otherwise. S3 `If-Match`, GCS generations, Azure ETags. Presenting no version means "only if absent". |
| **CAS chain** | The succession of version tokens one fence object passes through. A successful write must present the current token and hands back the next, so links cannot be skipped — holding the current token *is* ownership, and a writer that misses one can never rejoin. Defined in full on `RemoteStoreFence`. |
| **fencing token** | Whatever a writer has to present to prove it may still write. Here that is the current CAS token. |
| **seal**, or **claim** | Take the fence over at your own term: create your own key, then delete every lower-term key. |
| **sweep** | The delete step of a claim — removing every acknowledgement path below your own term. |
| **acknowledgement path** | The per-term fence object a writer advances on every ack. Deleting it is what stops a stale writer. |
| **superseded** | A strictly higher term now holds the fence, so this copy has to stop touching shared state. |
| **restore point** | The latest translog metadata a recovering copy replays from. |
| **hydration** | Downloading the segment files named by the latest segment metadata. |
| **orphan** | A metadata file that a fenced writer published but never acknowledged. Harmless, but it accumulates. |
| **partitioned writer** | A previous primary that has dropped out of the cluster's view but can still reach the object store. The case this protocol exists for. |

The two shard-data paths are named separately throughout rather than lumped together as "the data
flow", because they are fenced by different means — a CAS on the translog flow, an ownership check on
the segment flow — so which one is meant always matters.

## The invariants

State, for the seal-ordering module, which abstracts the acknowledgement
path as a single CAS register: a fence `F = (token, term, seq, owner)`;
writers `w ∈ W`, each with an appointed term `term(w)`, a held token
`held(w)`, a lifecycle state in `{unborn, fresh, sealed, active, fenced}`
and a restore point `restore(w)` read from the store; and a global set
`acked` of acknowledged operations, attributed by `ackedBy(w)`.

**Safety** — holds in every reachable state:

1. **Mutual exclusion.** At most one live writer holds the current token,
   and that writer is the recorded owner.

   `|{ w : state(w) ∈ {sealed, active} ∧ held(w) = F.token }| ≤ 1`

2. **No acked write loss.** The one that matters most. Once the owner has
   read its restore point, every acknowledged operation is either in that
   restore point or was acknowledged by the owner itself.

   `F.owner ≠ ⊥ ∧ hasRead(F.owner) ⇒ acked ⊆ restore(F.owner) ∪ ackedBy(F.owner)`

   Put another way: nothing a superseded writer acknowledged lands after
   the restore point its successor serves from.

3. **Ack attribution.** `⋃_w ackedBy(w) = acked`, and the `ackedBy(w)` are
   pairwise disjoint — every ack belongs to exactly one writer.

4. **Owner/term agreement.** `F.owner ≠ ⊥ ⇒ F.term = term(F.owner)`

**Action properties** — hold across every transition:

5. **Term monotonicity.** `□[F.term' ≥ F.term]_vars`
6. **Seq monotonicity.** `□[F.token' > F.token ⇒ F.seq' = F.seq + 1]_vars`
7. **Fenced is terminal.** `□[∀w : state(w) = fenced ⇒ state'(w) = fenced]_vars`

**Liveness** — under weak fairness of the claimant's read and CAS:

8. **A higher term prevails.**
   `∀w : (state(w) = fresh ∧ term(w) > F.term) ⇝ (F.term ≥ term(w) ∨ state(w) = fenced)`

These follow from five protocol rules:

- an ack is a successful CAS on the chain (*the chain gates the ack*);
- a takeover claims the chain **before** reading its restore point
  (*seal before restore*), and that ordering on its own is what makes (2)
  hold;
- a claimant below the fence term is refused, an equal or higher one may
  claim, and the CAS settles concurrent claimants (*the term never
  regresses*);
- ownership fields are advisory, and the token chain is the only authority
  (*the owner is advisory*);
- cluster coordination contributes exactly two properties — monotonic term
  issuance, and at most one active primary per shard — and is otherwise
  uninvolved (*no cluster-manager synchronization*). It appears in the
  models only through the `Appoint*` actions.

## What is proved, and how

**Exhaustive model checking with TLC** over bounded models, with a complete state graph in every
case and 0 states left on queue; see *On the bounds* below. Every reachable interleaving of
appointments, partitioned-writer acknowledgements, claim races, relocation handoff and abort,
segment publication and collection, and restore-point reads is checked. This is not a TLAPS
deductive proof for unbounded parameters. A TLAPS proof of mutual exclusion and no-acked-write-loss
via an inductive invariant is the natural follow-up if that is wanted.

Four modules, each modeling the protocol **as implemented**:

| Module | Question | Bound | States |
|---|---|---|---|
| `RemoteStoreFence.tla` | seal ordering and acked-write loss | 3 writers, 3 terms, 3 ops | 19,846 |
| `FenceTakeover.tla` | cross-term takeover: is a higher-term victory deterministic? | 4 writers, 4 terms, 3 ops, symmetry | 1,593,658 |
| `FenceHandoff.tla` | equal-term relocation handoff, retried relocation, concurrent higher-term takeover, target loss | 4 ops, 3 attempts | 591 |
| `FenceSegmentFlow.tla` | the segment flow as a second path needing the fence, plus garbage collection | 3 writers, 3 files, 3 terms | 20,008 |

### On the bounds

Each run explores its **complete** state graph — 0 states left on queue — so nothing is missed
within these bounds. Larger bounds were probed out of band:

- **Takeover saturates in `MaxTerm`.** With N writers the failover chain can reach at most term N,
  since each appointment consumes one unborn writer, so `MaxTerm ≥ |Writers|` adds no reachable
  behaviour. Confirmed exactly: 4 writers at `MaxTerm` 4, 5 and 8 each explore the *identical*
  1,593,658 distinct states (3,032,893 generated, 0 left on queue, depth 30), all clean.
  **Writers, not terms, is the scaling dimension.**
- **5 writers exceeds this machine**, not the protocol. The run was still exploring with ~18M
  states queued when it was stopped. That is a resource limit and *not* a violation: no invariant
  was reported violated at any bound.
- **Seal ordering** was verified at 4 writers / 4 terms / 4 ops out of band: 3,691,705 distinct
  states from 8,200,777 generated, 0 left on queue, all properties holding. The committed bound is
  smaller only because that module checks *liveness*, whose cost dominates state count.

> Every count here is TLC's **final** summary. Do not read the first `distinct states found` in TLC's
> output: that is a mid-run `Progress(...)` line and varies run to run, understating the total.
> `registerTlcTask` matches the final summary and fails the build if any states are left on queue.

Symmetry reduction (`SYMMETRY Symm`) is used only on `FenceTakeover`, where the specification is
symmetric in `Writers` and only invariants are checked. It gives roughly a 6× reduction, and it is
**not** sound with liveness properties in general.

### Cross-term takeover — `FenceTakeover.tla`

Determinism is stated as safety, which is what makes it exhaustively checkable:

**NoHigherTermDefeat** — a copy may only ever be defeated by a term at least as high as its own,
and never out-raced by a lower-term incumbent:

`∀w : state(w) = fenced ⇒ fencedBy(w) ≥ term(w)`

This holds because a writer's only destructive act is deleting objects **strictly below** its own
term, so a lower-term writer cannot touch a higher-term writer's acknowledgement path. Determinism
comes from the key space rather than from winning a race. Cluster coordination supplies the
authority — it issues the term, the term names the key, and create-if-absent plus name ordering let
the object store order grants it cannot interpret — while performing no I/O itself.

### Relocation handoff — `FenceHandoff.tla`

Relocation happens at a constant term, so source and target share one object and term-scoping says
nothing about them. The source still owns the chain and has drained its uploads, so it performs the
ownership transfer itself as its last act before handing off, and keeps the resulting token. The
target is authorized by **recorded ownership**. On abort the source attempts a revert with that
token: success means the target never wrote, so the source resumes; failure means the target took
over, so the source stands down. The object store settles an ambiguity the two copies cannot settle
by talking to each other.

**NoSpuriousFencing** — an aborted handoff must not fence a healthy source:

`state(source) = fenced ⇒ ackedBy(target) ≠ ∅`

together with the symmetric **TargetAcksAreDurable** — `ackedBy(target) ≠ ∅ ⇒ state(source) ≠ resumed`
— so a late revert cannot displace a target that already acknowledged.

### The segment flow and GC — `FenceSegmentFlow.tla`

A shard publishes segment metadata as well as translog metadata, and a recovering copy hydrates
from it, so remote state is read on both the translog flow and the segment flow, and both need the fence.

**HydrationIntegrity** — the copy that owns the fence must never find a file it still needs already
gone: whatever it listed is either still in the store or already fetched. **OwnerNeverReadUnsealed**
is the ordering twin, asserting that an owner never read remote state before it owned the fence.

Three requirements make these hold, and how load-bearing each one is was **measured** by relaxing it
against the model rather than argued. The answer is not uniform:

| Requirement relaxed | Result |
|---|---|
| Seal before read, back to the old code order — hydrate segments, then seal | `OwnerNeverReadUnsealed` **violated** |
| Ownership check on GC, alone | no violation |
| Ownership check on segment metadata publication, alone | no violation |
| **Both** ownership checks | `HydrationIntegrity` **violated** |

So seal-before-read is needed on its own, but the two ownership checks are each individually
*sufficient*: what the property needs is **at least one** of them, not both. The reason is visible in
the failure trace — one copy performs both the publish and the prune, so gating either action breaks
the chain. With publication gated the reference set cannot move under a hydrating owner; with
collection gated nothing prunes to a moved set.

The implementation keeps both regardless, for reasons the model cannot express rather than any it
proved. `Gc(w)` requires `wState = "active"`, so the model cannot represent collection at other
points in the real shard lifecycle; and in the code the two guards sit on different paths guarding
different resources — the refresh listener for segments, `trimUnreferencedReaders` for the translog.
That is defence in depth, not redundancy the model endorsed.

### What the segment flow does not cover

`FenceSegmentFlow.tla` appoints only at `HighestIssuedTerm + 1`, so every copy in it is either the
owner or **strictly** superseded. That bound is load-bearing rather than incidental: `Superseded(w)`
tests `fenceTerm > wTerm[w]`, so an **equal-term** copy is never superseded and both ownership gates
always pass for it. Adding an equal-term actor makes `HydrationIntegrity` itself false in 11 states —
the source publishes a second reference set and then collects, deleting a file the equal-term copy had
listed but not yet fetched. That was measured, not assumed.

It matters because the code does reach the segment flow at an equal term.
`sealRemoteStoreFenceBeforeRestore` exempts `PEER` recovery, and a primary relocation target recovers
over `PEER` — so it hydrates segments **without sealing**, while its same-term source keeps publishing
and collecting. **The fence does not protect that case**, and nothing here claims it does.

What protects it is metadata **retention**, not the fence: `deleteStaleSegmentsAsync` keeps the last N
metadata files and everything they reference, whereas `Gc` in the model prunes to the single latest
reference set. Modelling retention would turn this into a *bound* — safe while the source publishes
fewer than N times during the target's hydration — which is a rate argument rather than a guarantee.
That belongs in a module of its own alongside `FenceHandoff.tla`, and is an open follow-up rather than
something to fold in here at the cost of the cross-term result.

### On the gates being checked, not assumed

`Refresh` and `Gc` both require `~Superseded(w)`, which presumes the check is accurate. The
implementation's publish path **fails open** — an unreadable fence reports "not superseded" — and the
gate is evaluated once at the top of a sync that then runs for some time, so a copy can publish after
becoming superseded. Both are states the model forbids.

They are licensed by the necessity result above rather than by luck: relaxing the publication gate
*alone* yields no violation, because collection is gated — and the collection paths **fail closed**.
The asymmetry between the two failure directions is what makes the fail-open publish defensible, which
is the practical value of the "either one alone suffices" measurement.

### One property deliberately not asserted

TLC shows *"only the highest granted term may acknowledge"* is false of any correct protocol.
Issuing a grant does not revoke the incumbent — revocation happens when the successor takes the
acknowledgement path — so there is always a window where the previous owner still acknowledges
after its successor was appointed. That window is exactly what seal-before-restore makes safe,
which is what `NoAckedWriteLoss` asserts.

Alternative designs evaluated and refuted along the way — a single shared acknowledgement-path
object, a claim-object escalation scheme, and a target-claims handoff — were each refuted by TLC and
are not kept as live configurations.

## Layout

```
README.md                       this file: terms, the invariants, what is proved, how to run it
tla/RemoteStoreFence.tla        seal ordering and acked-write loss
tla/FenceTakeover.tla           cross-term takeover determinism
tla/FenceHandoff.tla            equal-term relocation handoff
tla/FenceSegmentFlow.tla        the segment flow as a second path needing the fence, plus GC
tla/*.cfg                       one TLC configuration per module
```

## Running

Model checking is **opt-in, and the build downloads nothing.** Supply `tla2tools.jar` yourself:

```bash
./gradlew :formal-models:modelCheck -PtlaToolsJar=/path/to/tla2tools.jar
# or
TLA_TOOLS_JAR=/path/to/tla2tools.jar ./gradlew :formal-models:modelCheck
```

Without a jar the tasks fail with instructions. Individual modules run as `:formal-models:tlcRemoteStoreFence`, `tlcFenceTakeover`, `tlcFenceHandoff`, `tlcFenceSegmentFlow`.

### Why it is not part of `check`

TLC ships in `tla2tools.jar`, which is not on Maven Central or any registry this build already
trusts, so obtaining it means fetching a GitHub release asset. Doing that and verifying a pinned
SHA-256 afterwards would not be good enough:

- A checksum pinned in the **same change** that introduces the download only proves the bytes are
  self-consistent. It cannot establish that the URL, the release asset, or the digest are the
  upstream project's — all three need corroborating out of band, by a person.
- Wired into `check`, every CI environment running `./gradlew check` would pull an unmanaged
  third-party binary. Supplementary verification of a design spec is not worth that blast radius,
  and it also broke air-gapped builds.

### Vetting the jar

There is **deliberately no pinned checksum**. A digest recorded by whoever added the reference cannot
vouch for itself, and a disclaimed pin sitting in the repository invites exactly the false confidence
it warns against. Because the caller supplies the
jar, comparing it against a number written here would establish nothing: if they vetted it the check
is redundant, and if they did not, this repository's digest cannot help.

Verifying the artifact is the caller's job, and belongs where the jar is obtained rather than where it
is used:

1. Obtain `tla2tools.jar` from the TLA+ project's own release channel.
2. Corroborate it against **upstream's own published checksum**, a distribution package, or a fetch
   from a different network and host.
3. Mirror the vetted jar into artifact storage the project already trusts, and have any CI job take
   it from there rather than from a third-party release URL.

Step 3 is where a checksum belongs, enforced by whatever guards that storage. This build takes the jar
it is given and does not pretend to have verified it.

Until that vetting happens, the exposure is limited by design: nothing is downloaded,
`:formal-models:check` does nothing, and the tasks fail closed unless a jar is passed explicitly.
