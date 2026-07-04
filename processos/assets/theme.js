// ProcessOS theme runtime — include SYNCHRONOUSLY in <head>, right after the
// /assets/theme.css link, on every page:
//
//   <link rel="stylesheet" href="/assets/theme.css">
//   <script src="/assets/theme.js"></script>
//
// Loading it blocking-in-head is deliberate: the appearance attribute must be
// on <html> before first paint or a light-mode user gets a dark flash.
//
// Modes: 'light' | 'dark' | 'system' (persisted as processos.theme; default
// 'system', which tracks prefers-color-scheme live). Pages mount the switcher
// with poMountThemeToggle(container).
(function () {
  var KEY = "processos.theme";
  var mq = window.matchMedia("(prefers-color-scheme: light)");

  function mode() {
    var m = localStorage.getItem(KEY);
    return m === "light" || m === "dark" ? m : "system";
  }

  function apply() {
    var m = mode();
    var appearance = m === "system" ? (mq.matches ? "light" : "dark") : m;
    document.documentElement.dataset.appearance = appearance;
    return appearance;
  }

  function set(m) {
    if (m === "system") localStorage.removeItem(KEY);
    else localStorage.setItem(KEY, m);
    apply();
    document.querySelectorAll(".po-theme-toggle").forEach(paint);
    // Let page code (charts, BPMN overlays, …) react to the resolved change.
    try {
      window.dispatchEvent(new CustomEvent("processos:theme", { detail: { mode: mode(), appearance: document.documentElement.dataset.appearance } }));
    } catch (e) {}
  }

  // Track the OS while in system mode.
  var onChange = function () {
    if (mode() === "system") set("system");
  };
  if (mq.addEventListener) mq.addEventListener("change", onChange);
  else if (mq.addListener) mq.addListener(onChange); // older engines

  var ICONS = {
    light:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>',
    dark:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>',
    system:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/></svg>',
  };
  var TITLES = { light: "Light", dark: "Dark", system: "Follow system" };

  function paint(el) {
    var m = mode();
    el.querySelectorAll("button").forEach(function (b) {
      b.classList.toggle("on", b.dataset.mode === m);
    });
  }

  /** Render the light/dark/system segmented control into `container`. */
  function mount(container) {
    if (!container) return;
    var el = document.createElement("div");
    el.className = "po-theme-toggle";
    ["light", "dark", "system"].forEach(function (m) {
      var b = document.createElement("button");
      b.type = "button";
      b.dataset.mode = m;
      b.title = TITLES[m];
      b.setAttribute("aria-label", TITLES[m]);
      b.innerHTML = ICONS[m];
      b.onclick = function () {
        set(m);
      };
      el.appendChild(b);
    });
    paint(el);
    container.appendChild(el);
    return el;
  }

  apply(); // before first paint (this script is blocking in <head>)

  window.poTheme = { mode: mode, set: set, apply: apply, appearance: function () { return document.documentElement.dataset.appearance; } };
  window.poMountThemeToggle = mount;
})();
