import { useEffect, useRef } from "react";

/**
 * The server landing page's rotating "constellation" particle field, ported to a
 * self-contained canvas component: points drift, the whole field slowly rotates
 * around the centre, and nearby points are linked by lines whose opacity falls
 * off with distance. Pure canvas, no dependencies. Sits behind everything.
 */
export function ParticleField() {
  const ref = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Respect prefers-reduced-motion: skip seeding/animating and hide the canvas
    // so users who disabled motion get no animated background and no wasted CPU.
    const reduce = window.matchMedia?.("(prefers-reduced-motion: reduce)");
    if (reduce?.matches) {
      canvas.style.display = "none";
      return;
    }

    let w = 0;
    let h = 0;
    let cx = 0;
    let cy = 0;
    let dpr = 1;
    let rot = 0;
    let raf = 0;
    type P = {
      a: number;
      r: number;
      vx: number;
      vy: number;
      x: number;
      y: number;
      driftX: number;
      driftY: number;
    };
    let points: P[] = [];

    function seed() {
      const count = Math.min(140, Math.floor((w * h) / (22000 * dpr)));
      points = [];
      for (let i = 0; i < count; i++) {
        points.push({
          a: Math.random() * Math.PI * 2,
          r: Math.pow(Math.random(), 0.6) * Math.min(w, h) * 0.55,
          vx: (Math.random() - 0.5) * 0.25 * dpr,
          vy: (Math.random() - 0.5) * 0.25 * dpr,
          x: 0,
          y: 0,
          driftX: 0,
          driftY: 0,
        });
      }
    }

    function resize() {
      dpr = Math.min(window.devicePixelRatio || 1, 2);
      w = canvas!.width = Math.floor(window.innerWidth * dpr);
      h = canvas!.height = Math.floor(window.innerHeight * dpr);
      canvas!.style.width = window.innerWidth + "px";
      canvas!.style.height = window.innerHeight + "px";
      cx = w / 2;
      cy = h / 2;
      seed();
    }

    const LINK = 130;
    function frame() {
      rot += 0.0006;
      ctx!.clearRect(0, 0, w, h);
      const cosR = Math.cos(rot);
      const sinR = Math.sin(rot);
      const link = LINK * dpr;

      for (const p of points) {
        const bx = Math.cos(p.a) * p.r;
        const by = Math.sin(p.a) * p.r;
        p.driftX += p.vx;
        p.driftY += p.vy;
        if (Math.abs(p.driftX) > 60 * dpr) p.vx *= -1;
        if (Math.abs(p.driftY) > 60 * dpr) p.vy *= -1;
        p.x = cx + (bx * cosR - by * sinR + p.driftX);
        p.y = cy + (bx * sinR + by * cosR + p.driftY);
      }

      for (let i = 0; i < points.length; i++) {
        for (let j = i + 1; j < points.length; j++) {
          const a = points[i];
          const b = points[j];
          const d = Math.hypot(a.x - b.x, a.y - b.y);
          if (d < link) {
            const t = 1 - d / link;
            ctx!.strokeStyle = `rgba(80, 200, 230, ${0.18 * t})`;
            ctx!.lineWidth = dpr * 0.6;
            ctx!.beginPath();
            ctx!.moveTo(a.x, a.y);
            ctx!.lineTo(b.x, b.y);
            ctx!.stroke();
          }
        }
      }

      for (const p of points) {
        ctx!.beginPath();
        ctx!.arc(p.x, p.y, dpr * 1.6, 0, Math.PI * 2);
        ctx!.fillStyle = "rgba(150, 230, 220, 0.85)";
        ctx!.fill();
      }
      raf = requestAnimationFrame(frame);
    }

    window.addEventListener("resize", resize);
    resize();
    frame();
    return () => {
      window.removeEventListener("resize", resize);
      cancelAnimationFrame(raf);
    };
  }, []);

  return <canvas id="field" ref={ref} />;
}
