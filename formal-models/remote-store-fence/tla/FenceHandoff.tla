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
(*     a fresh target, up to MaxAttempts times. So the source alternates    *)
(*     between serving and handing off, and each attempt sees the state the *)
(*     previous one left.                                                   *)
(*  2. A CONCURRENT HIGHER-TERM TAKEOVER. A node can be lost during the     *)
(*     relocation, promoting a copy at a higher term whose sweep DELETES    *)
(*     this term's shared object - invalidating the source's kept token and *)
(*     the target's adoption at once. This is the coupling between the two  *)
(*     protocols that modeling them separately cannot see.                  *)
(*  3. TARGET LOSS. A target can be lost after activating, either before or *)
(*     after it adopted the chain.                                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS MaxOps, MaxAttempts

VARIABLES
  fenceExists,  \* the shared object for this term; a higher-term sweep deletes it
  fenceOwner,   \* "source" | "target": who the object records as owner
  fenceToken,   \* bumped on every successful write
  srcToken,     \* token the source believes is current
  tgtToken,     \* token the target holds (-1 = none: it must read the object)
  srcState,     \* "serving" | "drained" | "transferred" | "resumed" | "standDown" | "fenced"
  tgtState,     \* "recovering" | "activated" | "serving" | "failed" | "lost"
  handoff,      \* "none" | "sent" | "accepted" | "lost"
  attempts,     \* relocation attempts started so far
  superseded,   \* a higher-term copy has taken over and swept this term's object
  acked, ackedBySource, ackedByTarget, nextOp

vars == <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, srcState, tgtState,
          handoff, attempts, superseded, acked, ackedBySource, ackedByTarget, nextOp>>

Init ==
  /\ fenceExists = TRUE /\ fenceOwner = "source" /\ fenceToken = 0
  /\ srcToken = 0 /\ tgtToken = -1
  /\ srcState = "serving" /\ tgtState = "recovering" /\ handoff = "none"
  /\ attempts = 0 /\ superseded = FALSE
  /\ acked = {} /\ ackedBySource = {} /\ ackedByTarget = {} /\ nextOp = 1

SourceOwns == fenceExists /\ fenceOwner = "source" /\ srcToken = fenceToken
TargetOwns == fenceExists /\ fenceOwner = "target" /\ tgtToken = fenceToken

(***************************************************************************)
(* The source serves until it drains. An acknowledgement is one successful  *)
(* CAS; losing it is terminal for that copy.                               *)
(***************************************************************************)
SourceAck ==
  /\ srcState \in {"serving", "resumed"}
  /\ nextOp <= MaxOps
  /\ IF SourceOwns
       THEN /\ fenceToken' = fenceToken + 1
            /\ srcToken' = fenceToken + 1
            /\ acked' = acked \cup {nextOp}
            /\ ackedBySource' = ackedBySource \cup {nextOp}
            /\ nextOp' = nextOp + 1
            /\ UNCHANGED <<fenceExists, fenceOwner, tgtToken, srcState, tgtState, handoff,
                           attempts, superseded, ackedByTarget>>
       ELSE /\ srcState' = "fenced"
            /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState,
                           handoff, attempts, superseded, acked, ackedBySource, ackedByTarget,
                           nextOp>>

(* Block operations, final sync, drain uploads: after this the source cannot upload. *)
Drain ==
  /\ srcState \in {"serving", "resumed"}
  /\ attempts < MaxAttempts
  /\ srcState' = "drained"
  /\ attempts' = attempts + 1
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, handoff,
                 superseded, acked, ackedBySource, ackedByTarget, nextOp>>

(* The current owner hands ownership over itself, uncontested. It has drained, so it is the only
   writer at this point, and the target may not claim until it observes the transfer. *)
TransferOwnership ==
  /\ srcState = "drained"
  /\ IF SourceOwns
       THEN /\ fenceOwner' = "target"
            /\ fenceToken' = fenceToken + 1
            /\ srcToken' = fenceToken + 1
            /\ srcState' = "transferred"
            /\ UNCHANGED <<fenceExists, tgtToken, tgtState, handoff, attempts, superseded,
                           acked, ackedBySource, ackedByTarget, nextOp>>
       ELSE \* Our own object is gone or taken: we have been superseded, so we cannot hand off.
         /\ srcState' = "fenced"
         /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState,
                        handoff, attempts, superseded, acked, ackedBySource, ackedByTarget,
                        nextOp>>

SendContext ==
  /\ handoff = "none"
  /\ srcState = "transferred"
  /\ handoff' = "sent"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, srcState, tgtState,
                 attempts, superseded, acked, ackedBySource, ackedByTarget, nextOp>>

(* The target receives the context and activates. It holds no token: it must read the object. *)
DeliverContext ==
  /\ handoff = "sent"
  /\ tgtState = "recovering"
  /\ tgtState' = "activated"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, srcState, handoff,
                 attempts, superseded, acked, ackedBySource, ackedByTarget, nextOp>>

(* The source's view of the handoff fails - a lost response, a timeout, a cancellation - even
   though the target may already have activated. *)
LoseHandoff ==
  /\ handoff = "sent"
  /\ handoff' = "lost"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, srcState, tgtState,
                 attempts, superseded, acked, ackedBySource, ackedByTarget, nextOp>>

CompleteHandoff ==
  /\ handoff = "sent"
  /\ tgtState = "activated"
  /\ handoff' = "accepted"
  /\ srcState' = "standDown"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState, attempts,
                 superseded, acked, ackedBySource, ackedByTarget, nextOp>>

(***************************************************************************)
(* The target picks the chain up by reading the object, and RECORDED        *)
(* OWNERSHIP is what authorizes it. If the source reverted, the object      *)
(* records the source, so the target stands down. If the object is gone, a  *)
(* higher term swept it, and the target stands down too.                    *)
(***************************************************************************)
TargetAdoptOwnership ==
  /\ tgtState = "activated"
  /\ tgtToken = -1
  /\ IF fenceExists /\ fenceOwner = "target"
       THEN /\ fenceToken' = fenceToken + 1
            /\ tgtToken' = fenceToken + 1
            /\ UNCHANGED <<fenceExists, fenceOwner, srcToken, srcState, tgtState, handoff,
                           attempts, superseded, acked, ackedBySource, ackedByTarget, nextOp>>
       ELSE /\ tgtState' = "failed"
            /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, srcState,
                           handoff, attempts, superseded, acked, ackedBySource, ackedByTarget,
                           nextOp>>

TargetAck ==
  /\ tgtState \in {"activated", "serving"}
  /\ tgtToken /= -1
  /\ nextOp <= MaxOps
  /\ IF TargetOwns
       THEN /\ fenceToken' = fenceToken + 1
            /\ tgtToken' = fenceToken + 1
            /\ tgtState' = "serving"
            /\ acked' = acked \cup {nextOp}
            /\ ackedByTarget' = ackedByTarget \cup {nextOp}
            /\ nextOp' = nextOp + 1
            /\ UNCHANGED <<fenceExists, fenceOwner, srcToken, srcState, handoff, attempts,
                           superseded, ackedBySource>>
       ELSE /\ tgtState' = "failed"
            /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, srcState,
                           handoff, attempts, superseded, acked, ackedBySource, ackedByTarget,
                           nextOp>>

(* The target is lost - node failure - whether or not it had adopted the chain. *)
LoseTarget ==
  /\ tgtState \in {"activated", "serving"}
  /\ tgtState' = "lost"
  /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, srcState, handoff,
                 attempts, superseded, acked, ackedBySource, ackedByTarget, nextOp>>

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
  /\ IF fenceExists /\ srcToken = fenceToken
       THEN \* The target never wrote, so it never took over: reclaim and resume.
         /\ fenceOwner' = "source"
         /\ fenceToken' = fenceToken + 1
         /\ srcToken' = fenceToken + 1
         /\ srcState' = "resumed"
         /\ handoff' = "none"
         /\ tgtState' = "lost"
         /\ tgtToken' = -1
         /\ UNCHANGED <<fenceExists, attempts, superseded, acked, ackedBySource, ackedByTarget,
                        nextOp>>
       ELSE \* Either the target wrote, or a higher term swept the object: stand down.
         /\ srcState' = "standDown"
         /\ UNCHANGED <<fenceExists, fenceOwner, fenceToken, srcToken, tgtToken, tgtState,
                        handoff, attempts, superseded, acked, ackedBySource, ackedByTarget,
                        nextOp>>

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
  /\ UNCHANGED <<fenceOwner, fenceToken, srcToken, tgtToken, srcState, tgtState, handoff,
                 attempts, acked, ackedBySource, ackedByTarget, nextOp>>

Next ==
  \/ SourceAck \/ Drain \/ TransferOwnership \/ SendContext \/ DeliverContext
  \/ LoseHandoff \/ CompleteHandoff \/ TargetAdoptOwnership \/ TargetAck
  \/ LoseTarget \/ AbortHandoff \/ HigherTermTakeover

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* SAFETY                                                                  *)
(***************************************************************************)

SourceLive == srcState \in {"serving", "resumed"} /\ SourceOwns
TargetLive == tgtState \in {"activated", "serving"} /\ TargetOwns

MutualExclusion == ~(SourceLive /\ TargetLive)

(* The property this module exists for: an aborted handoff must not fence a healthy source. The
   source may only end up fenced if the target actually acknowledged something, meaning the
   handoff genuinely took effect, or if a higher term legitimately superseded it. *)
NoSpuriousFencing == srcState = "fenced" => (ackedByTarget /= {} \/ superseded)

(* Symmetric obligation: a target that acknowledged must never be displaced by a late revert,
   unless a higher term took the shard from both of them. *)
TargetAcksAreDurable == (ackedByTarget /= {} /\ ~superseded) => srcState /= "resumed"

AckAttribution ==
  /\ ackedBySource \cup ackedByTarget = acked
  /\ ackedBySource \cap ackedByTarget = {}

=================================================================================
