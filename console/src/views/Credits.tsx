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
// container) rather than a CSS transform, so the reader can scroll manually at
// any time and reduced-motion is honoured. Like a film end-roll, the content
// begins *below* the viewport (a full-height top spacer) and rises up from the
// bottom edge, giving the title card a beat to be read before it moves.
//
// An atmospheric ambient-electronic bed plays while the roll is open: "Impact
// Prelude" by Kevin MacLeod (incompetech.com), Creative Commons BY 4.0 — hence
// the attribution in the roll itself. It is best-effort (streamed, looped) and
// muteable; browsers that block autoplay simply start silent until the toggle.

const SPEEDS = [0.5, 1, 1.5, 2] as const;
const PX_PER_SEC = 42;

// "Impact Prelude" — Kevin MacLeod (incompetech.com), CC BY 4.0. Streamed as a
// looping ambient bed; a plain <audio src> needs no CORS for playback.
const MUSIC_URL =
  "https://incompetech.com/music/royalty-free/mp3-royaltyfree/Impact%20Prelude.mp3";

export default function Credits() {
  const reduced =
    typeof window !== "undefined" &&
    window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;

  const [playing, setPlaying] = useState(!reduced);
  const [speed, setSpeed] = useState<number>(1);
  const [musicOn, setMusicOn] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const spacerRef = useRef<HTMLDivElement>(null);
  const audioRef = useRef<HTMLAudioElement>(null);

  // Keep the latest playing/speed in refs so the rAF loop (started once) reads
  // current values without re-subscribing.
  const playingRef = useRef(playing);
  const speedRef = useRef(speed);
  playingRef.current = playing;
  speedRef.current = speed;

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;

    // Size the top spacer to a full viewport of the scroll container so the
    // roll starts empty and the first card rises in from the bottom edge. For
    // reduced motion (no auto-scroll), jump past the spacer so content is
    // immediately readable at the top instead of showing an empty screen.
    const sizeSpacer = () => {
      if (spacerRef.current) {
        spacerRef.current.style.height = `${el.clientHeight}px`;
      }
    };
    sizeSpacer();
    if (!playingRef.current) el.scrollTop = el.clientHeight;
    const ro = new ResizeObserver(sizeSpacer);
    ro.observe(el);

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
            el.scrollTop = 0; // loop the roll (back to the empty bottom-start)
          } else {
            el.scrollTop = Math.min(el.scrollTop + whole, max);
          }
        }
      }
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
    };
  }, []);

  // Best-effort: try to start the ambient bed on mount (navigating here is a
  // user gesture, which many browsers accept). If autoplay is blocked, the
  // catch leaves it off and the ♪ control lets the reader start it.
  useEffect(() => {
    const audio = audioRef.current;
    if (!audio) return;
    audio.volume = 0.55;
    audio
      .play()
      .then(() => setMusicOn(true))
      .catch(() => setMusicOn(false));
    return () => {
      audio.pause();
    };
  }, []);

  const toggleMusic = () => {
    const audio = audioRef.current;
    if (!audio) return;
    if (audio.paused) {
      audio.volume = 0.55;
      audio
        .play()
        .then(() => setMusicOn(true))
        .catch(() => setMusicOn(false));
    } else {
      audio.pause();
      setMusicOn(false);
    }
  };

  const restart = () => {
    if (scrollRef.current) scrollRef.current.scrollTop = 0;
    setPlaying(true);
  };

  return (
    <div className="relative h-full overflow-hidden bg-app text-fg">
      {/* subtle vignette top/bottom so text fades in/out at the edges */}
      <div className="pointer-events-none absolute inset-x-0 top-0 z-10 h-20 bg-gradient-to-b from-app to-transparent" />
      <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10 h-20 bg-gradient-to-t from-app to-transparent" />

      {/* controls */}
      <div className="absolute right-4 top-4 z-20 flex items-center gap-2 text-xs">
        <button
          onClick={toggleMusic}
          title={musicOn ? "Mute ambient music" : "Play ambient music"}
          className={`rounded-md border px-3 py-1.5 ${
            musicOn
              ? "border-accent bg-accent/20 text-accent-strong"
              : "border-edge-strong bg-panel/80 text-fg-muted hover:bg-hover"
          }`}
        >
          {musicOn ? "♪ Music" : "♪ Muted"}
        </button>
        <button
          onClick={() => setPlaying((p) => !p)}
          className="rounded-md border border-edge-strong bg-panel/80 px-3 py-1.5 text-fg-muted hover:bg-hover"
        >
          {playing ? "Pause" : "Play"}
        </button>
        <button
          onClick={restart}
          className="rounded-md border border-edge-strong bg-panel/80 px-3 py-1.5 text-fg-muted hover:bg-hover"
        >
          Restart
        </button>
        <div className="flex overflow-hidden rounded-md border border-edge-strong">
          {SPEEDS.map((s) => (
            <button
              key={s}
              onClick={() => setSpeed(s)}
              className={`px-2 py-1.5 ${
                speed === s
                  ? "bg-accent/20 text-accent-strong"
                  : "bg-panel/80 text-fg-muted hover:bg-hover"
              }`}
            >
              {s}×
            </button>
          ))}
        </div>
      </div>

      <audio ref={audioRef} src={MUSIC_URL} loop preload="none" />

      <div ref={scrollRef} className="h-full overflow-y-auto">
        <div className="mx-auto flex w-full max-w-4xl flex-col items-center px-6 pb-40 text-center">
          {/* Full-viewport spacer so the roll starts empty and the title card
              rises up from the bottom edge (sized imperatively in the effect). */}
          <div ref={spacerRef} aria-hidden="true" className="w-full shrink-0" />

          {/* Title card */}
          <div className="pt-16">
            <div className="text-7xl font-semibold tracking-tight sm:text-8xl">
              nano BPM
            </div>
            <div className="mt-5 text-lg uppercase tracking-[0.35em] text-accent-strong">
              An Advanced Research Prototype
            </div>
            <p className="mx-auto mt-10 max-w-2xl text-xl leading-relaxed text-fg-muted">
              Nano is an Advanced Research Prototype, incorporating a decade of
              experience and expertise of Camunda Engineering.
            </p>

            <div className="mx-auto mt-16 max-w-2xl">
              <p className="text-3xl italic leading-relaxed text-fg sm:text-4xl">
                He aha te mea nui o te ao?
                <br />
                He tāngata, he tāngata, he tāngata
              </p>
              <p className="mx-auto mt-7 max-w-xl text-xl leading-relaxed text-fg-muted">
                What is the most precious thing in the world?
                <br />
                It is people, it is people, it is people
              </p>
              <p className="mt-6 text-sm uppercase tracking-[0.25em] text-fg-faint">
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
            <p className="mb-6 max-w-lg text-sm leading-relaxed text-fg-faint">
              {credits.counts.total} contributors across the Camunda platform,
              Zeebe, FEEL, and the bpmn-io modeling toolkit — everyone whose
              work Nano stands on.
            </p>
            <div className="grid grid-cols-2 gap-x-10 gap-y-2 text-lg text-fg-muted sm:grid-cols-3">
              {credits.cast.map((n) => (
                <div key={n} className="truncate">
                  {n}
                </div>
              ))}
            </div>
          </Section>

          <Gap />

          <div className="pt-8 text-center">
            <div className="text-base uppercase tracking-[0.3em] text-fg-faint">
              With gratitude
            </div>
            <div className="mt-5 text-4xl font-semibold tracking-tight text-fg">
              Camunda Engineering
            </div>
            <div className="mx-auto mt-10 max-w-md text-sm text-fg-faint">
              Names are engraved inside the machine, the way the Macintosh and
              Amiga teams signed their cases.
            </div>
            <div className="mx-auto mt-8 max-w-md text-xs leading-relaxed text-fg-faint">
              Music: “Impact Prelude” by Kevin MacLeod (incompetech.com) ·
              Licensed under Creative Commons: By Attribution 4.0
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
      <h2 className="mb-8 text-sm font-bold uppercase tracking-[0.3em] text-fg-faint">
        {title}
      </h2>
      <div className="flex w-full flex-col items-center gap-7">{children}</div>
    </section>
  );
}

function BigName({ name, sub }: { name: string; sub?: string }) {
  return (
    <div>
      <div className="text-3xl font-medium tracking-tight text-fg sm:text-4xl">
        {name}
      </div>
      {sub && <div className="mt-1.5 text-sm text-fg-faint">{sub}</div>}
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
      <div className="text-sm font-semibold uppercase tracking-widest text-accent-strong">
        {lead}
      </div>
      <div className="mt-1.5 text-3xl font-medium tracking-tight text-fg sm:text-4xl">
        {name}
      </div>
      {sub && <div className="mt-1 text-sm text-fg-faint">{sub}</div>}
    </div>
  );
}

function Gap() {
  return <div className="h-28 shrink-0" aria-hidden="true" />;
}
