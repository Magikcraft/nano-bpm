import { useEffect, useRef, useState } from "react";
import { credits } from "./creditsData";

// The Nano console "Credits" — a movie-style end-roll that acknowledges the
// Camunda engineers whose work Nano distills. Nano is called an Advanced
// Research Prototype, but Zeebe was the research prototype; Nano is the
// distillation of nine years of Camunda Engineering. Artists sign their work,
// so we do too: subsystems carry their creators' names, and this roll credits
// the wider community whose contributions live on across the product.
//
// The scroll is driven imperatively (requestAnimationFrame over a scrollable
// container) rather than a CSS transform, so the content is always visible
// from the top — even when paused or when the OS requests reduced motion — and
// the reader can scroll manually at any time.

const SPEEDS = [0.5, 1, 1.5, 2] as const;
const PX_PER_SEC = 42;

export default function Credits() {
  const reduced =
    typeof window !== "undefined" &&
    window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;

  const [playing, setPlaying] = useState(!reduced);
  const [speed, setSpeed] = useState<number>(1);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Keep the latest playing/speed in refs so the rAF loop (started once) reads
  // current values without re-subscribing.
  const playingRef = useRef(playing);
  const speedRef = useRef(speed);
  playingRef.current = playing;
  speedRef.current = speed;

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    let raf = 0;
    let last = performance.now();
    // Sub-pixel accumulator so slow speeds still advance smoothly.
    let acc = 0;
    const step = (now: number) => {
      const dt = Math.min(now - last, 100) / 1000;
      last = now;
      if (playingRef.current) {
        acc += dt * PX_PER_SEC * speedRef.current;
        const whole = Math.floor(acc);
        if (whole > 0) {
          acc -= whole;
          const max = el.scrollHeight - el.clientHeight;
          if (max <= 0) {
            // content not measured yet; try again next frame
          } else if (el.scrollTop >= max - 1) {
            el.scrollTop = 0; // loop the roll
          } else {
            el.scrollTop = Math.min(el.scrollTop + whole, max);
          }
        }
      }
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, []);

  const restart = () => {
    if (scrollRef.current) scrollRef.current.scrollTop = 0;
    setPlaying(true);
  };

  return (
    <div className="relative h-full overflow-hidden bg-black text-zinc-100">
      {/* subtle vignette top/bottom so text fades in/out at the edges */}
      <div className="pointer-events-none absolute inset-x-0 top-0 z-10 h-20 bg-gradient-to-b from-black to-transparent" />
      <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10 h-20 bg-gradient-to-t from-black to-transparent" />

      {/* controls */}
      <div className="absolute right-4 top-4 z-20 flex items-center gap-2 text-xs">
        <button
          onClick={() => setPlaying((p) => !p)}
          className="rounded-md border border-zinc-700 bg-zinc-900/80 px-3 py-1.5 text-zinc-300 hover:bg-zinc-800"
        >
          {playing ? "Pause" : "Play"}
        </button>
        <button
          onClick={restart}
          className="rounded-md border border-zinc-700 bg-zinc-900/80 px-3 py-1.5 text-zinc-300 hover:bg-zinc-800"
        >
          Restart
        </button>
        <div className="flex overflow-hidden rounded-md border border-zinc-700">
          {SPEEDS.map((s) => (
            <button
              key={s}
              onClick={() => setSpeed(s)}
              className={`px-2 py-1.5 ${
                speed === s
                  ? "bg-violet-500/30 text-violet-200"
                  : "bg-zinc-900/80 text-zinc-400 hover:bg-zinc-800"
              }`}
            >
              {s}×
            </button>
          ))}
        </div>
      </div>

      <div ref={scrollRef} className="h-full overflow-y-auto">
        <div className="mx-auto flex w-full max-w-3xl flex-col items-center px-6 pb-40 text-center">
          {/* Title card */}
          <div className="pt-16">
            <div className="text-5xl font-semibold tracking-tight">nano BPM</div>
            <div className="mt-3 text-sm uppercase tracking-[0.35em] text-violet-300">
              An Advanced Research Prototype
            </div>
            <p className="mx-auto mt-8 max-w-xl text-sm leading-relaxed text-zinc-400">
              Nano is an Advanced Research Prototype, incorporating a decade of
              experience and expertise of Camunda Engineering.
            </p>

            <div className="mx-auto mt-12 max-w-xl">
              <p className="text-base italic leading-relaxed text-zinc-200">
                He aha te mea nui o te ao?
                <br />
                He tangata, he tangata, he tangata
              </p>
              <p className="mx-auto mt-5 max-w-lg text-sm leading-relaxed text-zinc-400">
                What is the most precious thing in the world?
                <br />
                It is people, it is people, it is people
              </p>
              <p className="mt-4 text-xs uppercase tracking-[0.25em] text-zinc-600">
                Māori tikanga · New Zealand
              </p>
            </div>
          </div>

          <Gap />

          <Section title="Produced by">
            <BigName name={credits.producer.name} sub={credits.producer.role} />
          </Section>

          <Gap />

          <Section title="Signed Subsystems">
            {credits.signed.map((s) => (
              <NamedLine
                key={s.subsystem}
                lead={s.subsystem}
                name={s.name}
                sub={s.area}
              />
            ))}
          </Section>

          <Gap />

          <Section title="Founders">
            {credits.founders.map((f) => (
              <BigName key={f.name} name={f.name} sub={f.area} />
            ))}
          </Section>

          <Gap />

          <Section title="Principal Engineers">
            {credits.principals.map((p) => (
              <BigName key={p.name} name={p.name} sub={p.area} />
            ))}
          </Section>

          <Gap />

          <Section title="Modeling — bpmn-io">
            {credits.modeling.map((m) => (
              <BigName key={m.name} name={m.name} sub={m.area} />
            ))}
          </Section>

          <Gap />

          <Section title="The Camunda Engineering Community">
            <p className="mb-6 max-w-lg text-xs leading-relaxed text-zinc-500">
              {credits.counts.total} contributors across the Camunda platform,
              Zeebe, FEEL, and the bpmn-io modeling toolkit — everyone whose work
              Nano stands on.
            </p>
            <div className="grid grid-cols-2 gap-x-10 gap-y-1.5 text-sm text-zinc-300 sm:grid-cols-3">
              {credits.cast.map((n) => (
                <div key={n} className="truncate">
                  {n}
                </div>
              ))}
            </div>
          </Section>

          <Gap />

          <div className="pt-8 text-center">
            <div className="text-sm uppercase tracking-[0.3em] text-zinc-500">
              With gratitude
            </div>
            <div className="mt-4 text-2xl font-semibold tracking-tight text-zinc-200">
              Camunda Engineering
            </div>
            <div className="mt-8 text-xs text-zinc-600">
              Names are engraved inside the machine, the way the Macintosh and
              Amiga teams signed their cases.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="flex w-full flex-col items-center">
      <h2 className="mb-6 text-xs font-bold uppercase tracking-[0.3em] text-zinc-500">
        {title}
      </h2>
      <div className="flex w-full flex-col items-center gap-5">{children}</div>
    </section>
  );
}

function BigName({ name, sub }: { name: string; sub?: string }) {
  return (
    <div>
      <div className="text-xl font-medium tracking-tight text-white">
        {name}
      </div>
      {sub && <div className="mt-1 text-xs text-zinc-500">{sub}</div>}
    </div>
  );
}

function NamedLine({
  lead,
  name,
  sub,
}: {
  lead: string;
  name: string;
  sub?: string;
}) {
  return (
    <div className="flex flex-col items-center">
      <div className="text-xs font-semibold uppercase tracking-widest text-violet-300">
        {lead}
      </div>
      <div className="mt-1 text-xl font-medium tracking-tight text-white">
        {name}
      </div>
      {sub && <div className="mt-0.5 text-xs text-zinc-500">{sub}</div>}
    </div>
  );
}

function Gap() {
  return <div className="h-28 shrink-0" aria-hidden="true" />;
}
