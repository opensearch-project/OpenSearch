------------------------------ MODULE FenceSegmentFlow ------------------------------
(***************************************************************************)
(* The SEGMENT FLOW as a second path needing the fence, and garbage         *)
(* collection.                                                              *)
(*                                                                          *)
(* Three names first, since the rest of this leans on them, and they are    *)
(* kept distinct because each is fenced by different means:                 *)
(*                                                                          *)
(*   TRANSLOG FLOW - translog files and their metadata. The acknowledgement *)
(*     path, gated directly by the fence CAS.                               *)
(*   SEGMENT FLOW  - segment files and their metadata. What a recovering    *)
(*     copy hydrates from, gated by an ownership check rather than a CAS.   *)
(*   CONTROL FLOW  - decisions about who may write. The fence object is the *)
(*     only thing on it, and nothing reading shard data ever looks at it.   *)
(*                                                                          *)
(* "Data flow" as an umbrella for the first two is avoided below: which of  *)
(* the two is meant always matters here.                                    *)
(*                                                                          *)
(* The fence CAS gates the translog flow. But a shard also publishes        *)
(* SEGMENT metadata, and a recovering copy hydrates from it - downloads the *)
(* files it names. So a copy reads remote state on BOTH flows, and both     *)
(* must be read only after it owns the fence.                               *)
(*                                                                          *)
(* Segment metadata is keyed by primary term and read highest-term-first,   *)
(* so a superseded copy's metadata is skipped ONCE the new copy has         *)
(* published its own. During hydration it has not published yet, so the new *)
(* copy hydrates FROM the superseded copy's metadata. That is safe for      *)
(* acknowledged data, because every acknowledged operation is in the        *)
(* translog and therefore in the post-seal restore point. But it does mean  *)
(* the two copies touch the same files at once, and one of them may be      *)
(* running GARBAGE COLLECTION.                                              *)
(*                                                                          *)
(* GC deletes segment files the latest metadata no longer references. If a  *)
(* superseded copy may run it, it can delete files a hydrating copy has     *)
(* already listed and is still fetching. The property below says exactly    *)
(* that must not happen: hydration must never find a file it still needs    *)
(* gone.                                                                    *)
(*                                                                          *)
(* THREE requirements make it hold. How much each one is load-bearing is    *)
(* measured under NECESSITY below, and the answer is not uniform:           *)
(*                                                                          *)
(*   1. Seal before reading EITHER flow. Sealing only before the translog   *)
(*      restore-point read leaves segment hydration unprotected.            *)
(*   2. GC requires fence ownership. A copy that no longer owns the fence   *)
(*      must not delete shared state.                                       *)
(*   3. Publishing segment metadata requires fence ownership. This is the   *)
(*      least obvious one, and TLC found it. Term-scoped naming stops       *)
(*      READERS following a superseded copy's metadata, but until the new   *)
(*      copy publishes its own, that metadata is still the latest - and it  *)
(*      defines the reference set GC prunes to. So an unfenced publish by a *)
(*      superseded copy makes the LEGITIMATE owner's GC delete files that   *)
(*      owner is still fetching.                                            *)
(*                                                                          *)
(* NECESSITY, measured rather than asserted. Each requirement was relaxed   *)
(* in turn against this same model:                                         *)
(*                                                                          *)
(*  - Relax 1 to the old code order - hydrate segments, then seal - and     *)
(*    OwnerNeverReadUnsealed breaks. So 1 is needed on its own, for the     *)
(*    ordering property.                                                    *)
(*  - Relax 2 alone: nothing breaks. Relax 3 alone: nothing breaks. Relax   *)
(*    BOTH and HydrationIntegrity breaks. So each of 2 and 3 is             *)
(*    individually SUFFICIENT here, and what the property needs is AT LEAST *)
(*    ONE of them, not both. The example shows why: a single copy performs  *)
(*    both the publish and the prune, so gating either action breaks the    *)
(*    chain. With publication gated the reference set cannot move under a   *)
(*    hydrating owner; with collection gated nothing prunes to a moved set. *)
(*                                                                          *)
(*    The implementation keeps both regardless, for reasons this model      *)
(*    cannot express rather than any it proved. Gc(w) below requires wState *)
(*    = "active", so the model cannot represent collection at other points  *)
(*    in the real shard lifecycle. And in the code the two guards sit on    *)
(*    different paths guarding different resources: the refresh listener    *)
(*    for segments, trimUnreferencedReaders for the translog. Defence in    *)
(*    depth, not redundancy the model endorsed.                             *)
(* SCOPE: STRICTLY INCREASING TERMS ONLY. Appoint issues HighestIssuedTerm  *)
(* + 1, so every copy here is either the owner or strictly superseded. That *)
(* is not an oversight to be widened: Superseded(w) tests fenceTerm >       *)
(* wTerm[w], so an EQUAL-term copy is never superseded and both ownership   *)
(* gates always pass for it. Add an equal-term actor and HydrationIntegrity *)
(* itself becomes false in 11 states - the source publishes a second        *)
(* reference set and then collects, deleting a file the equal-term copy     *)
(* listed but had not fetched. Verified by trying it, not assumed.          *)
(*                                                                          *)
(* That matters because the code does reach the segment flow at an equal    *)
(* term: sealRemoteStoreFenceBeforeRestore exempts PEER recovery, and a     *)
(* primary relocation target recovers over PEER, so it hydrates segments    *)
(* WITHOUT sealing while its source keeps publishing and collecting. THE    *)
(* FENCE DOES NOT PROTECT THAT CASE, and nothing here claims it does.       *)
(*                                                                          *)
(* What protects it is metadata RETENTION rather than the fence:            *)
(* deleteStaleSegmentsAsync keeps the last N metadata files and everything  *)
(* they reference, where Gc below prunes to the single latest reference     *)
(* set. Modeling retention would turn this into a bound - safe while the    *)
(* source publishes fewer than N times during the target's hydration -      *)
(* which is a rate argument rather than a guarantee, and belongs in a       *)
(* module of its own alongside FenceHandoff.tla rather than weakening the   *)
(* cross-term result proved here.                                           *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
  Writers,    \* shard-copy incarnations
  Files,      \* segment file identities
  NoWriter,
  MaxTerm

ASSUME NoWriter \notin Writers

VARIABLES
  fenceOwner,   \* the copy that owns the fence, or NoWriter
  fenceTerm,    \* the term it owns it at
  latestTerm,   \* term of the latest published segment metadata (0 = none)
  latestRefs,   \* files referenced by that metadata
  files,        \* segment files present in the store
  wTerm,        \* term this copy was appointed at
  wState,       \* "unborn" | "fresh" | "sealed" | "hydrating" | "active" | "fenced"
  wNeeded,      \* files this copy listed for hydration
  wFetched,     \* files it has fetched so far
  wReadUnsealed \* TRUE if this copy read remote state before it owned the fence

vars == <<fenceOwner, fenceTerm, latestTerm, latestRefs, files, wTerm, wState, wNeeded, wFetched,
          wReadUnsealed>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

(* The gate the implementation actually applies is a POSITIVE test for supersession - has a strictly
   higher term taken the fence - and not a test that this copy exactly owns it. That difference
   matters in practice. A copy can hold a fence instance that is merely BEHIND the fence object,
   because engine resets during recovery replace the translog, and so the fence, several times in
   quick succession. An exact-ownership test answers "not owner" for that stale-but-healthy case
   as well, and would silence a live shard for good - hanging snapshot-restore recovery, since an
   engine reset there leaves a healthy copy holding a behind-but-valid fence. *)
Superseded(w) == fenceTerm > wTerm[w]
HighestIssuedTerm == Max({wTerm[v] : v \in Writers} \cup {fenceTerm})

Init ==
  /\ fenceOwner = NoWriter /\ fenceTerm = 0
  /\ latestTerm = 0 /\ latestRefs = {} /\ files = {}
  /\ wTerm = [w \in Writers |-> 0]
  /\ wState = [w \in Writers |-> "unborn"]
  /\ wNeeded = [w \in Writers |-> {}]
  /\ wFetched = [w \in Writers |-> {}]
  /\ wReadUnsealed = [w \in Writers |-> FALSE]

(* A copy is appointed at a strictly higher term. Appointing it does NOT stop any existing copy.
   That copy, still acting, is the partitioned writer. *)
Appoint(w) ==
  /\ wState[w] = "unborn"
  /\ HighestIssuedTerm + 1 <= MaxTerm
  /\ wTerm' = [wTerm EXCEPT ![w] = HighestIssuedTerm + 1]
  /\ wState' = [wState EXCEPT ![w] = "fresh"]
  /\ UNCHANGED <<fenceOwner, fenceTerm, latestTerm, latestRefs, files, wNeeded, wFetched, wReadUnsealed>>

(* Requirement 1: the seal comes before reading either flow, translog or segment. A claim below
   the fence term is refused, and that refusal is what makes a superseded copy stop. *)
Seal(w) ==
  /\ wState[w] = "fresh"
  /\ IF wTerm[w] >= fenceTerm
       THEN /\ fenceOwner' = w
            /\ fenceTerm' = wTerm[w]
            /\ wState' = [wState EXCEPT ![w] = "sealed"]
            /\ UNCHANGED <<latestTerm, latestRefs, files, wTerm, wNeeded, wFetched, wReadUnsealed>>
       ELSE /\ wState' = [wState EXCEPT ![w] = "fenced"]
            /\ UNCHANGED <<fenceOwner, fenceTerm, latestTerm, latestRefs, files, wTerm,
                           wNeeded, wFetched, wReadUnsealed>>

(* Hydration, on the segment flow: list the latest segment metadata, then fetch its files one at
   a time. Allowed only after sealing, which is requirement 1. *)
Hydrate(w) ==
  /\ wState[w] = "sealed"
  /\ wNeeded' = [wNeeded EXCEPT ![w] = latestRefs]
  /\ wState' = [wState EXCEPT ![w] = "hydrating"]
  /\ wReadUnsealed' = [wReadUnsealed EXCEPT ![w] = (fenceOwner /= w)]
  /\ UNCHANGED <<fenceOwner, fenceTerm, latestTerm, latestRefs, files, wTerm, wFetched>>

Fetch(w) ==
  /\ wState[w] = "hydrating"
  /\ \E f \in wNeeded[w] \ wFetched[w] :
       /\ f \in files
       /\ wFetched' = [wFetched EXCEPT ![w] = wFetched[w] \cup {f}]
  /\ UNCHANGED <<fenceOwner, fenceTerm, latestTerm, latestRefs, files, wTerm, wState, wNeeded, wReadUnsealed>>

FinishHydration(w) ==
  /\ wState[w] = "hydrating"
  /\ wNeeded[w] \subseteq wFetched[w]
  /\ wState' = [wState EXCEPT ![w] = "active"]
  /\ UNCHANGED <<fenceOwner, fenceTerm, latestTerm, latestRefs, files, wTerm, wNeeded, wFetched, wReadUnsealed>>

(* Requirement 3: publishing segment metadata requires fence ownership.
   Term-scoped naming on its own is NOT enough here. READERS do skip a superseded copy's
   metadata once the new copy publishes its own, but until then it is still the latest, and it
   defines the reference set that GC prunes to. So an unfenced publish by a superseded copy makes
   the LEGITIMATE owner's own GC delete files that owner is still fetching. Uploading segment
   DATA files unfenced stays harmless because it is purely additive. It is publishing the
   reference set that has to be gated. *)
Refresh(w) ==
  /\ wState[w] = "active"
  /\ ~Superseded(w)
  /\ \E newRefs \in (SUBSET Files) \ {{}} :
       /\ files' = files \cup newRefs
       /\ latestTerm' = wTerm[w]
       /\ latestRefs' = newRefs
  /\ UNCHANGED <<fenceOwner, fenceTerm, wTerm, wState, wNeeded, wFetched, wReadUnsealed>>

(* Requirement 2: garbage collection deletes files the latest metadata no longer references, and
   only the copy that owns the fence may run it. *)
Gc(w) ==
  /\ wState[w] = "active"
  /\ ~Superseded(w)
  /\ files' = files \cap latestRefs
  /\ UNCHANGED <<fenceOwner, fenceTerm, latestTerm, latestRefs, wTerm, wState, wNeeded, wFetched, wReadUnsealed>>

Next ==
  \E w \in Writers :
    \/ Appoint(w) \/ Seal(w) \/ Hydrate(w) \/ Fetch(w) \/ FinishHydration(w)
    \/ Refresh(w) \/ Gc(w)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* SAFETY                                                                  *)
(***************************************************************************)

(* The hydration of the copy that OWNS the fence must never find a file it still needs already
   gone: whatever it listed is either still in the store or already fetched.
   Scoped to the owner on purpose. A SUPERSEDED copy losing files mid-hydration is the correct
   outcome, not a defect - its recovery has to fail, and it must not become primary. *)
HydrationIntegrity ==
  \A w \in Writers :
    (wState[w] = "hydrating" /\ fenceOwner = w) => wNeeded[w] \subseteq (files \cup wFetched[w])

(* The owner, once hydrated, holds every file it listed. *)
HydrationComplete ==
  \A w \in Writers :
    (wState[w] = "active" /\ fenceOwner = w) => wNeeded[w] \subseteq wFetched[w]

(* Mutual exclusion over shared state is structural here rather than asserted. Both mutating
   actions - publishing metadata and collecting garbage - require fence ownership, and there is
   only one owner. A copy whose engine is open but no longer owns the fence can serve reads and
   mutates nothing. What IS worth asserting is the consequence: the latest published segment
   metadata was never published by a superseded copy. *)
LatestPublishedByOwner == latestTerm > 0 => latestTerm <= fenceTerm

(* Requirement 1, stated as ordering rather than as a data property: a copy that owns the fence
   must never have read remote state before it owned it. This is the specification-level twin of
   the runtime assertion at the read choke point in IndexShard. *)
OwnerNeverReadUnsealed == \A w \in Writers : fenceOwner = w => wReadUnsealed[w] = FALSE

=================================================================================
