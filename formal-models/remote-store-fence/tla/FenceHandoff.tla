------------------------------- MODULE FenceHandoff -------------------------------
(***************************************************************************)
(* Primary relocation handoff, as implemented.                              *)
(*                                                                          *)
(* Relocation happens at a CONSTANT primary term, so source and target      *)
(* share one acknowledgement-path object. Term-scoping, which is what makes *)
(* cross-term takeover deterministic, says nothing here - so ordering is    *)
(* the handoff protocol's job.                                              *)
(*                                                                          *)
(* Modeled after IndexShard#relocated. The source blocks operations, does a *)
(* final translog sync, DRAINS uploads, hands the primary context to the    *)
(* target over the network, and on failure ABORTS, releasing the drains so  *)
(* that it RESUMES as primary. The abort path is what makes this hard: the  *)
(* cluster keeps the source, so a source that got fenced in the meantime    *)
(* fails the shard for no reason.                                           *)
(*                                                                          *)
(* The protocol. The source still owns the chain and has drained, so it     *)
(* performs the ownership transfer itself as its last act before handing    *)
(* off, and keeps the resulting token. The target picks the chain up by     *)
(* reading the object, authorized by RECORDED OWNERSHIP rather than by a    *)
(* token carried in the primary context, so no wire-format change is        *)
(* needed. On abort the source attempts a REVERT with the token it kept:    *)
(*                                                                          *)
(*   revert succeeds => the target never wrote, so never took over: resume. *)
(*   revert fails    => the target did write, so the handoff effectively    *)
(*                      completed: stand down.                              *)
(*                                                                          *)
(* This makes the owner field AUTHORIZING within a term, where the CAS      *)
(* chain cannot arbitrate because both copies are legitimate. Across terms  *)
(* it stays advisory.                                                       *)
(*                                                                          *)
(* THREE things widen this beyond a single happy-path handoff:              *)
(*                                                                          *)
(*  1. RETRIED RELOCATION. After an abort the cluster may relocate again to *)
(*     a fresh target, up to MaxAttempts times. Attempt a hands off to      *)
(*     target a, and the object records the owner by identity, as the real *)
(*     object records an allocation id. Targets of earlier attempts are     *)
(*     not retired: one that activated may still try to take the chain.    *)
(*  2. A CONCURRENT HIGHER-TERM TAKEOVER. A node can be lost during the     *)
(*     relocation, promoting a copy at a higher term whose sweep DELETES    *)
(*     this term's shared object - invalidating the source's kept token and *)
(*     the target's adoption at once. This is the coupling between the two  *)
(*     protocols that modeling them separately cannot see.                  *)
(*  3. TARGET LOSS. A target can be lost after activating, either before or *)
(*     after it adopted the chain.                                          *)
(*                                                                          *)
(* An acknowledgement READS the object; only transfer, adoption and revert *)
(* write it. A fourth case explains the one ack that writes:                *)
(*                                                                          *)
(*  4. A REVERT THAT FAILS WITHOUT LANDING. The source resumes              *)
(*     ("reclaiming") on an object that still records the target, so its   *)
(*     first ack writes itself back as owner. As a plain read, TLC finds    *)
(*     acked-write loss: see the README.                                    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS MaxOps, MaxAttempts

Targets == 1..MaxAttempts   \* target a is the target of relocation attempt a
Source == 0                 \* the owner the object records for the source

VARIABLES
  fenceExists,   \* the shared object for this term; a higher-term sweep deletes it
  fenceOwner,    \* Source or a target: who the object records as owner
  fenceToken,    \* bumped on every successful write
  srcToken,      \* token the source believes is current
  srcState,      \* "serving" | "drained" | "transferred" | "resumed" | "reclaiming" | "standDown"
                 \* | "fenced". "reclaiming" is "resumed" after a revert that did not land: the
                 \* source's own last write still records the target.
  tgtToken,      \* per target: the token it holds (-1 = none: it must read the object)
  tgtState,      \* per target: "recovering" | "activated" | "serving" | "failed" | "lost"
  tgtRestore,    \* per target: the acknowledged operations its primary context carries
  ackedByTarget, \* per target: the operations it acknowledged
  handoff,       \* "none" | "sent" | "accepted" | "lost": the current attempt's handoff
  attempts,      \* relocation attempts started so far; the current one hands off to target attempts
  superseded,    \* a higher-term copy has taken over and swept this term's object
  acked, ackedBySource, nextOp

vars == <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtState, tgtRestore,
          ackedByTarget, handoff, attempts, superseded, acked, ackedBySource, nextOp>>

Init ==
  /\ fenceExists = TRUE /\ fenceOwner = Source /\ fenceToken = 0 /\ srcToken = 0
  /\ srcState = "serving"
  /\ tgtToken = [a \in Targets |-> -1] /\ tgtState = [a \in Targets |-> "recovering"]
  /\ tgtRestore = [a \in Targets |-> {}] /\ ackedByTarget = [a \in Targets |-> {}]
  /\ handoff = "none" /\ attempts = 0 /\ superseded = FALSE
  /\ acked = {} /\ ackedBySource = {} /\ nextOp = 1

(* What a conditional write or a fence read can see: the object exists and carries the token of
   this copy's own last write. Which owner that write recorded is the copy's own knowledge, not
   something the token check tests. *)
SourceOwns == fenceExists /\ srcToken = fenceToken
TargetOwns(a) == fenceExists /\ fenceOwner = a /\ tgtToken[a] = fenceToken

(***************************************************************************)
(* The source serves until it drains. An acknowledgement is a READ of the   *)
(* object; a changed or missing token is terminal for that copy. The one    *)
(* exception is the first ack of a "reclaiming" source, which writes its    *)
(* own ownership back - by CAS, so it cannot overwrite a target that        *)
(* adopted in the meantime.                                                 *)
(***************************************************************************)
SourceAck ==
  /\ srcState \in {"serving", "resumed", "reclaiming"}
  /\ nextOp <= MaxOps
  /\ IF SourceOwns
       THEN /\ IF srcState = "reclaiming"
                 THEN /\ fenceOwner' = Source
                      /\ fenceToken' = fenceToken + 1
                      /\ srcToken' = fenceToken + 1
                      /\ srcState' = "resumed"
                 ELSE UNCHANGED <<fenceOwner, fenceToken, srcToken, srcState>>
            /\ acked' = acked \cup {nextOp}
            /\ ackedBySource' = ackedBySource \cup {nextOp}
            /\ nextOp' = nextOp + 1
            /\ UNCHANGED <<fenceExists, tgtToken, tgtState, tgtRestore, ackedByTarget, handoff, attempts,
                           superseded>>
       ELSE /\ srcState' = "fenced"
            /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, tgtRestore,
                           ackedByTarget, handoff, attempts, superseded, acked, ackedBySource, nextOp>>

(* Block operations, final sync, drain uploads: after this the source cannot upload. *)
Drain ==
  /\ srcState \in {"serving", "resumed", "reclaiming"}
  /\ attempts < MaxAttempts
  /\ srcState' = "drained"
  /\ attempts' = attempts + 1
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, tgtRestore,
                 ackedByTarget, handoff, superseded, acked, ackedBySource, nextOp>>

(* The current owner hands ownership over itself, uncontested, naming this attempt's target. It has
   drained, so it is the only writer at this point, and the target may not claim until it observes
   the transfer. *)
TransferOwnership ==
  /\ srcState = "drained"
  /\ IF SourceOwns
       THEN /\ fenceOwner' = attempts
            /\ fenceToken' = fenceToken + 1
            /\ srcToken' = fenceToken + 1
            /\ srcState' = "transferred"
            /\ UNCHANGED <<fenceExists, tgtToken, tgtState, tgtRestore, ackedByTarget, handoff, attempts,
                           superseded, acked, ackedBySource, nextOp>>
       ELSE \* Our own object is gone or taken: we have been superseded, so we cannot hand off.
         /\ srcState' = "fenced"
         /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, tgtRestore,
                        ackedByTarget, handoff, attempts, superseded, acked, ackedBySource, nextOp>>

SendContext ==
  /\ handoff = "none"
  /\ srcState = "transferred"
  /\ handoff' = "sent"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtState, tgtRestore,
                 ackedByTarget, attempts, superseded, acked, ackedBySource, nextOp>>

(* This attempt's target receives the context and activates. It holds no token: it must read the
   object. *)
DeliverContext ==
  /\ handoff = "sent"
  /\ tgtState[attempts] = "recovering"
  /\ tgtState' = [tgtState EXCEPT ![attempts] = "activated"]
  /\ tgtRestore' = [tgtRestore EXCEPT ![attempts] = acked]
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, ackedByTarget, handoff,
                 attempts, superseded, acked, ackedBySource, nextOp>>

(* The source's view of the handoff fails - a lost response, a timeout, a cancellation - even
   though the target may already have activated. *)
LoseHandoff ==
  /\ handoff = "sent"
  /\ handoff' = "lost"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtState, tgtRestore,
                 ackedByTarget, attempts, superseded, acked, ackedBySource, nextOp>>

CompleteHandoff ==
  /\ handoff = "sent"
  /\ tgtState[attempts] = "activated"
  /\ handoff' = "accepted"
  /\ srcState' = "standDown"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, tgtRestore,
                 ackedByTarget, attempts, superseded, acked, ackedBySource, nextOp>>

(***************************************************************************)
(* A target picks the chain up by reading the object, and RECORDED          *)
(* OWNERSHIP is what authorizes it. If the source reverted, or a later      *)
(* attempt named another target, the object does not record this one, so   *)
(* it stands down. If the object is gone, a higher term swept it, and the   *)
(* target stands down too.                                                  *)
(***************************************************************************)
TargetAdoptOwnership(a) ==
  /\ tgtState[a] = "activated"
  /\ tgtToken[a] = -1
  /\ IF fenceExists /\ fenceOwner = a
       THEN /\ fenceToken' = fenceToken + 1
            /\ tgtToken' = [tgtToken EXCEPT ![a] = fenceToken + 1]
            /\ UNCHANGED <<fenceExists, fenceOwner, srcToken, srcState, tgtState, tgtRestore, ackedByTarget,
                           handoff, attempts, superseded, acked, ackedBySource, nextOp>>
       ELSE /\ tgtState' = [tgtState EXCEPT ![a] = "failed"]
            /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtRestore,
                           ackedByTarget, handoff, attempts, superseded, acked, ackedBySource, nextOp>>

TargetAck(a) ==
  /\ tgtState[a] \in {"activated", "serving"}
  /\ tgtToken[a] /= -1
  /\ nextOp <= MaxOps
  /\ IF TargetOwns(a)
       THEN /\ tgtState' = [tgtState EXCEPT ![a] = "serving"]
            /\ acked' = acked \cup {nextOp}
            /\ ackedByTarget' = [ackedByTarget EXCEPT ![a] = @ \cup {nextOp}]
            /\ nextOp' = nextOp + 1
            /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtRestore,
                           handoff, attempts, superseded, ackedBySource>>
       ELSE /\ tgtState' = [tgtState EXCEPT ![a] = "failed"]
            /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtRestore,
                           ackedByTarget, handoff, attempts, superseded, acked, ackedBySource, nextOp>>

(* A target is lost - node failure - whether or not it had adopted the chain. *)
LoseTarget(a) ==
  /\ tgtState[a] \in {"activated", "serving"}
  /\ tgtState' = [tgtState EXCEPT ![a] = "lost"]
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtRestore,
                 ackedByTarget, handoff, attempts, superseded, acked, ackedBySource, nextOp>>

(***************************************************************************)
(* Abort. The cluster keeps the source as primary and releases the drains,  *)
(* so the source resumes - which is why it must not have been fenced in    *)
(* the meantime. The revert uses the token that TransferOwnership kept.     *)
(***************************************************************************)
AbortHandoff ==
  \* "none" as well as "lost": the implementation reclaims on ANY failure after the transfer, which
  \* includes startRelocationHandoff throwing before the context was ever sent. That case is strictly
  \* easier - the target cannot have written - but it is a reachable path, so model it rather than
  \* leaving it outside the spec.
  /\ handoff \in {"none", "lost"}
  /\ srcState = "transferred"
  /\ IF SourceOwns
       THEN \* The target never wrote, so it never took over: reclaim and resume. The target is left
            \* running; the object no longer records it, so it cannot take the chain.
         /\ fenceOwner' = Source
         /\ fenceToken' = fenceToken + 1
         /\ srcToken' = fenceToken + 1
         /\ srcState' = "resumed"
         /\ handoff' = "none"
         /\ UNCHANGED <<fenceExists, tgtToken, tgtState, tgtRestore, ackedByTarget, attempts, superseded,
                        acked, ackedBySource, nextOp>>
       ELSE \* Either the target wrote, or a higher term swept the object: stand down.
         /\ srcState' = "standDown"
         /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, tgtRestore,
                        ackedByTarget, handoff, attempts, superseded, acked, ackedBySource, nextOp>>

(* The revert fails WITHOUT landing - the request never reached the store - and the copy cannot tell
   that apart from a lost response, so the caller keeps it as primary and releases its drains. The
   object still records the target, and the target is not stopped: it may already have activated. *)
AbortHandoffUnresolved ==
  /\ handoff \in {"none", "lost"}
  /\ srcState = "transferred"
  /\ srcState' = "reclaiming"
  /\ handoff' = "none"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, tgtRestore,
                 ackedByTarget, attempts, superseded, acked, ackedBySource, nextOp>>

(***************************************************************************)
(* A node is lost mid-relocation and a copy is promoted at a HIGHER term.   *)
(* Its sweep deletes this term's shared object, invalidating the source's   *)
(* retained token and the target's adoption at the same time. This is the   *)
(* coupling between the takeover and handoff protocols.                     *)
(***************************************************************************)
HigherTermTakeover ==
  /\ superseded = FALSE
  /\ superseded' = TRUE
  /\ fenceExists' = FALSE
  /\ UNCHANGED <<fenceOwner, fenceToken, srcToken, srcState, tgtToken, tgtState, tgtRestore, ackedByTarget,
                 handoff, attempts, acked, ackedBySource, nextOp>>

Next ==
  \/ SourceAck \/ Drain \/ TransferOwnership \/ SendContext \/ DeliverContext
  \/ LoseHandoff \/ CompleteHandoff \/ AbortHandoff \/ AbortHandoffUnresolved \/ HigherTermTakeover
  \/ \E a \in Targets : TargetAdoptOwnership(a) \/ TargetAck(a) \/ LoseTarget(a)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* SAFETY                                                                  *)
(***************************************************************************)

SourceLive == srcState \in {"serving", "resumed", "reclaiming"} /\ SourceOwns
TargetLive(a) == tgtState[a] \in {"activated", "serving"} /\ TargetOwns(a)

MutualExclusion ==
  /\ ~(SourceLive /\ \E a \in Targets : TargetLive(a))
  /\ \A a, b \in Targets : a /= b => ~(TargetLive(a) /\ TargetLive(b))

(* The property this module exists for: an aborted handoff must not fence a healthy source. The
   source may only end up fenced if a target actually took the chain over, meaning a handoff
   genuinely took effect, or if a higher term legitimately superseded it. Taking over is adoption,
   not a first ack: after an unresolved revert, a target that activated may adopt before the
   source's reclaim, and the handoff has then taken effect even if the target has not acked yet. *)
NoSpuriousFencing == srcState = "fenced" => ((\E a \in Targets : tgtToken[a] /= -1) \/ superseded)

(* Nothing anyone else acknowledged is missing from the state a serving target recovered with. The
   target serves from the primary context it received, so an op acknowledged after that - on a chain
   the target can still adopt - would be lost. *)
NoAckedWriteLoss == \A a \in Targets : TargetLive(a) => (acked \ ackedByTarget[a]) \subseteq tgtRestore[a]

(* Symmetric obligation: a target that acknowledged must never be displaced by a late revert,
   unless a higher term took the shard from both of them. *)
TargetAcksAreDurable == ((\E a \in Targets : ackedByTarget[a] /= {}) /\ ~superseded) => srcState /= "resumed"

AckAttribution ==
  /\ ackedBySource \cup UNION {ackedByTarget[a] : a \in Targets} = acked
  /\ \A a \in Targets : ackedBySource \cap ackedByTarget[a] = {}

=================================================================================
