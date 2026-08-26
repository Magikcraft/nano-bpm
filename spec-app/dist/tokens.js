var n=["app","panel","raised","inset","hover","edge","edgeStrong","text","textMuted","textFaint","accent","accentStrong","accent2","onAccent","ok","warn","danger","info"],r={dark:{app:"#0b0b10",panel:"#10101a",raised:"#16161f",inset:"#08080c",hover:"#1e1e2a",edge:"#24242f",edgeStrong:"#383848",text:"#f2f2f7",textMuted:"#a3a3b2",textFaint:"#6e6e80",accent:"#8b5cf6",accentStrong:"#a78bfa",accent2:"#22d3ee",onAccent:"#ffffff",ok:"#34d399",warn:"#fbbf24",danger:"#fb7185",info:"#38bdf8"},light:{app:"#f5f5f9",panel:"#fdfdfe",raised:"#ffffff",inset:"#ededf3",hover:"#e8e8f0",edge:"#e2e2ea",edgeStrong:"#c5c5d4",text:"#1a1a22",textMuted:"#565664",textFaint:"#8c8c9c",accent:"#7c3aed",accentStrong:"#6d28d9",accent2:"#0891b2",onAccent:"#ffffff",ok:"#059669",warn:"#b45309",danger:"#e11d48",info:"#0369a1"}};function c(t){return`--nano-${t.replace(/[A-Z2]/g,e=>e==="2"?"-2":`-${e.toLowerCase()}`)}`}function i(t){return n.includes(t)}var s=`/*
 * GENERATED from src/tokens.ts \u2014 do not edit by hand.
 *
 * The canonical --nano-* colour palette (issue #1005), as CSS custom properties.
 * Regenerate with \`npm run build\` (from spec-app/). The single source of truth is
 * NANO_PALETTE in src/tokens.ts; this file and the ./tokens map cannot drift (the
 * package build regenerates this file and a test locks it to the map).
 *
 * A theme is switched by \`data-appearance\` on <html>: dark is the default :root,
 * light applies under [data-appearance="light"]. Theme packs override the same
 * properties inline on <html>.
 */`;function o(t){return n.map(e=>`  ${c(e)}: ${r[t][e]};`).join(`
`)}function d(t={}){let e=[s,`:root,
:root[data-appearance="dark"] {
  color-scheme: dark;

${o("dark")}
}`,`:root[data-appearance="light"] {
  color-scheme: light;

${o("light")}
}`];return t.standaloneFallback&&e.push(`@media (prefers-color-scheme: light) {
  /* Standalone only: follow the OS until a host sets data-appearance. */
  :root:not([data-appearance]) {
    color-scheme: light;

`+n.map(a=>`    ${c(a)}: ${r.light[a]};`).join(`
`)+`
  }
}`),`${e.join(`

`)}
`}export{r as NANO_PALETTE,n as TOKEN_KEYS,i as isTokenKey,d as renderPaletteCss,c as tokenCssVar};
