# ADR 0015 — Artists sign their work

Status: **Accepted — 2026-07-05.**
Relates to: `console/src/views/Credits.tsx` (end-roll UI), `console/src/views/creditsData.ts`
(the roster), `docs/adr/0005-embedded-u-nano.md` (Bernd — the first named subsystem
under this policy), `docs/adr/0002-leader-local-activation-and-lease-digest.md`
("Deepthi" — the single-writer engine actor), `docs/falcon-design.md` (Falcon protocol).

## Context

Nano is not a from-scratch design. It is a distillation of nine years of Camunda
engineering — the single-writer engine, the Raft-replicated log, the FEEL
expression language, the BPMN semantics, the modeling toolkit — and, older than
that, the ideas that made workflow-as-code a durable pattern at all: Saga and
compensation, event sourcing, the actor model, streaming state machines. Every
one of those ideas has an author, or a small group of authors, whose work Nano
literally *runs*.

The Nano console already carries this on its face. `Credits.tsx` is a
movie-style end-roll that lists Camunda engineers by contribution area, and
several major subsystems already carry the names of their originators:

- **Falcon** — the command-stream protocol, after **Falko Menge**.
- **Deepthi** — the single-writer engine actor, after **Deepthi Devaki
  Akkoorath**.
- **Bernd** — the embedded engine, after **Bernd Ruecker** (ADR 0005).
- **FEEL** area credited to **Philipp Ossler**; DMN (reserved) to **Sebastian
  Menski**.

What has been implicit — "we name things after the people whose work they are"
— is now made explicit as a project policy, so that future subsystems inherit it
without needing to rediscover the reasoning each time.

### Prior art (the practice we are copying)

- **Macintosh (1982)** — Steve Jobs had the 47 members of the Mac team sign the
  inside of the case mould. Every Macintosh shipped 1982–1986 carries those
  signatures on the inside of the enclosure, invisible in normal use. Jobs told
  the team: *"Real artists sign their work."*
- **Amiga 1000 (1985)** — Jay Miner and the Amiga team signed the inside of the
  A1000 top cover (including Miner's dog Mitchy's pawprint). Same instinct, same
  year-adjacent, independent of Apple.

Nano's credits roll is the same gesture, moved to a place a user can actually
find it: not moulded into a case they will never open, but scrollable inside
the product itself.

### The Picasso quote (and why we take it seriously)

*"Good artists copy, great artists steal."* — attributed to Picasso, popularized
by Steve Jobs in the *Triumph of the Nerds* interview.

The distinction matters: copying leaves the source visible and untransformed;
stealing internalizes the idea until it becomes yours to reshape. Nano steals in
this sense. It does not fork Zeebe. It does not vendor Camunda code. It
re-implements the *ideas* — the single-writer actor, the leader-local
activation, the streaming protocol, the multi-partition Raft log, the FEEL
expression grammar — in a new codebase with a different shape, different
trade-offs, and different constraints (single-node-first, embeddable,
zero-dependency `engine-core`).

Stealing well obligates you to credit the source. Copying does not — you can
just leave the source visible. Since Nano internalizes and reshapes, it owes an
explicit acknowledgement, and the acknowledgement is not a footnote: it is a
first-class product surface.

## Decision

**Nano subsystems are named after the people whose work they realize, and
the Nano console carries an end-roll crediting the wider community whose
contributions live on across the product.**

This is a policy, not a one-off. It has four rules:

### 1. Subsystem naming — sign the load-bearing pieces

When a subsystem is a direct descendant of a specific person's identifiable
work, it takes their name (or a name derived from it). Examples in place:

| Nano subsystem | Named after | For |
| --- | --- | --- |
| **Falcon** | Falko Menge | The command-stream protocol design |
| **Deepthi** | Deepthi Devaki Akkoorath | The single-writer engine actor model |
| **Bernd** | Bernd Ruecker | The embedded engine (ADR 0005) — the Saga/compensation intellectual heritage that made "same source, embedded or remote" tenable |

Candidates for future signing (illustrative, not committed):

- The DMN decision engine, if/when it lands, is reserved for **Sebastian
  Menski**.
- The FEEL implementation credits **Philipp Ossler**; if a distinct FEEL
  subsystem emerges (e.g. a compiler), it may carry his name explicitly.
- Any future exporter framework acknowledges **Nicolas Pepin-Perreault**.
- Any Raft/journal work acknowledges **Lena Schönburg** and **Deepthi Devaki
  Akkoorath**.

The criteria for signing (all four must hold):

1. **A specific person or small named group is the identifiable intellectual
   source** — not a diffuse "the community".
2. **The subsystem realizes their idea, not merely uses their code.** Nano does
   not run Camunda code, so signing is about ideas realized, not code
   inherited.
3. **The subsystem is a real architectural boundary** — a package, a service, a
   protocol, a runtime — not an internal utility.
4. **The person's work is publicly attributable** (papers, talks, open-source
   commits, or a track of design authorship visible in the Camunda/Zeebe git
   history).

The last criterion is what keeps this from becoming arbitrary tribute: the
naming has to point at *work you can go read*.

### 2. The credits roll is a product feature

`console/src/views/Credits.tsx` — the movie-style end-roll — is not a marketing
page or an About dialog. It is a first-class view of the console with the same
polish budget as any other view. Its properties:

- **Reachable** from the console UI without spelunking.
- **Complete** — the roster in `creditsData.ts` is derived from git contributor
  history across the Camunda/Zeebe/bpmn-io repositories, deduped, bots removed,
  and regenerated rather than hand-edited. Every human contributor whose work
  Nano stands on gets a line.
- **Alive** — an ambient audio bed plays while the roll is open (respecting
  reduced-motion and browser autoplay policies).
- **Signed** — the "signed" section explicitly names subsystems and their
  authors, mirroring the case-mould signatures of the Macintosh and the A1000.
- **Regenerable** — treat `creditsData.ts` as generated; contributor additions
  come from re-running the generator against updated git history, not from
  editing the file by hand. This keeps the roster honest and drift-free.

### 3. Attribution in the codebase mirrors attribution in the product

- Subsystem-level `README.md` files and top-of-file doc comments name the
  author when the subsystem is signed (e.g. Bernd's `README` opens with the
  same signature block as ADR 0005 §"Signature — why 'Bernd'").
- ADRs that establish a signed subsystem contain a "Signature" section
  explaining who the person is and why the work is theirs (ADR 0005 §"Signature
  — why 'Bernd'" is the reference template).
- Prior-art acknowledgements in commit messages and PR descriptions are
  encouraged when a change realizes a specific published idea (paper, talk,
  blog post, upstream design doc).

### 4. Naming is respectful and consented where possible

- **Living people we can reach**: we ask before we sign. Falco Menge, Deepthi
  Devaki Akkoorath, Bernd Ruecker, Philipp Ossler and Sebastian Menski are all
  reachable; adding a name to the signed roster should be accompanied by an
  attempt to notify (a courtesy DM, an email, a PR mention). If someone asks
  not to be named, we remove the name and either rename the subsystem or use
  an area-based name instead.
- **We do not sign in a way that implies endorsement of Nano by the person
  named** — the signature says *"this subsystem realizes their work"*, not
  *"they endorse this project"*. The Signature blocks make this explicit.
- **The producer signature is separate.** `credits.producer` names the person
  responsible for the Nano project as a whole (currently Joshua Wulf); it is a
  different act from subsystem signing, and it is not an act of stealing
  because it names the person who took the responsibility of stealing well.

## Consequences

### What this buys us

- **Honesty about lineage.** Nano is described as an "Advanced Research
  Prototype", but the research prototype was Zeebe. Nano is the distillation.
  Signing the work makes that visible in the product itself, so nobody has to
  guess where the ideas came from.
- **A test we can apply to future subsystem names.** When a new subsystem lands
  and someone proposes a name, "does this pass the four signing criteria?" is a
  concrete question with a concrete answer.
- **A cultural signal to contributors.** Nano treats prior art as inheritance,
  not raw material. That framing propagates to how new work is discussed,
  reviewed and credited.
- **A tie to the physical-computing tradition** — Macintosh case moulds and
  Amiga A1000 top covers — that is part of what makes computing feel like a
  craft rather than a supply chain.

### Honest limits

- **Signing can drift into hagiography.** The criteria in §1 exist so that the
  signature list stays load-bearing. If we sign everything, we sign nothing.
- **Attribution is not code provenance.** Signing a subsystem after a person
  does not mean any of their code is in Nano (in general, none is). Nano's
  LICENSE, dependency tree, and re-implementation posture stand on their own;
  the signature is about ideas.
- **Living-person naming has ongoing consent implications.** If a named person
  later asks to be removed, we honour that promptly. The naming policy has to
  be revocable to be respectful.
- **Auto-generated rosters miss people.** `creditsData.ts` is generated from
  git contributor history and will miss people whose contributions were not
  through commits (designers, PMs, community leaders, talk-givers). Manual
  additions to the generator input are appropriate and should be reviewed.

### Non-goals

- **Not a licensing statement.** Signing is orthogonal to license compatibility;
  it does not grant, imply, or require any license from the named person.
- **Not a hiring or organizational statement.** Signing an ex-Camunda engineer's
  name onto a subsystem is a technical-heritage acknowledgement, not an
  affiliation claim.
- **Not permanent immutability.** Names can be added when new subsystems land,
  and removed on request. The policy is durable; the roster is not.
- **Not a substitute for financial or contractual acknowledgements.** Where
  work has commercial or contractual implications (e.g. patents, trademarks),
  those go through separate channels; signing is the *cultural* layer only.

## Options considered

- **A. No policy (status quo pre-this-ADR).** Signing happens ad hoc; each new
  subsystem re-argues the case. Rejected: the reasoning is stable and worth
  writing down once so the *practice* is durable.
- **B. Sign every subsystem after somebody.** Rejected: violates §1 criterion
  1 and cheapens the existing signatures.
- **C. Credits page but no subsystem naming.** Rejected: this is what most
  projects do (a `CONTRIBUTORS.md` or a `docs/credits.md`). It puts the
  acknowledgement somewhere users never look. The Macintosh/Amiga precedent is
  precisely that the signatures live *inside* the product.
- **D. Subsystem naming but no credits roll.** Rejected: the signed subsystems
  are the specific load-bearing pieces, but Nano's inheritance is *wider* than
  the few signed pieces — the roll credits the whole community whose work made
  the field.

## Open questions

- **Contributor consent workflow.** Today the roster is generated from public
  git history without explicit per-person consent. This is standard in
  open-source attribution, but Nano's roll is more prominent than a
  `CONTRIBUTORS.md`. We should offer an opt-out mechanism (a `credits-optout`
  list read by the generator) and document how to request removal.
- **Non-code contributors.** How do we surface designers, technical writers,
  and community leaders whose contributions were not commits? A manual
  addendum in `creditsData.ts` with the same schema is the obvious answer;
  formalize it.
- **Third-party dependency authors.** Nano depends on FOSS libraries whose
  authors are not in the Camunda git history. The current roll does not
  include them; should it? (Probably yes, in a distinct section, generated
  from `Cargo.lock` / `package.json` maintainer metadata where available.)

## Signature

This ADR is signed by the Nano producer, **Joshua Wulf**, and dedicated to the
Macintosh team of 1982 and the Amiga team of 1985 — whose signatures are still
inside those cases, forty years later.

*Real artists sign their work.*
