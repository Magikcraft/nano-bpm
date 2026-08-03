import {
  lazy,
  Suspense,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  NavLink,
  Navigate,
  Route,
  Routes,
  useLocation,
} from "react-router-dom";
import Topology from "./views/Topology";
import { useTheme } from "./theme/ThemeProvider";
import { getExtensions, getMarketplace, getTopology } from "./gen";
import { registerFileTypesFromOverview } from "./lib/editorLang";
import { setIntellisenseFromOverview } from "./lib/langIntellisense";
import { IS_STUDIO, CONSOLE_PROFILE } from "./lib/profile";
import { useProductTour } from "./lib/tour/useProductTour";
import { TourContext } from "./lib/tour/tourContext";
import { navAnchor, TOUR_ANCHOR } from "./lib/tour/tourAnchors";
import { pickerJourneys } from "./lib/tour/picker";
import StartupJourneyPanel from "./components/StartupJourneyPanel";
import ChangelogPanel from "./components/ChangelogPanel";
import type { ChangelogDoc } from "./lib/changelog";
import { hasUnseenSince, normalizeVersion } from "./lib/changelog";
import { RouteErrorBoundary } from "./components/RouteErrorBoundary";

// Route views are code-split so heavy editors (bpmn-js modeler + properties
// panel, monaco) stay out of the initial bundle and load on navigation.
//
// The studio-only views are additionally guarded by the compile-time
// `__STUDIO__` literal (ADR 0034): in an `observe` build it folds to `false`, so
// esbuild drops these `import()` anchors during transform and the IDE chunks
// (Monaco's ts.worker/typescript, the bpmn/dmn/form modeler bundle) — and the
// orphan `?worker` bundles Vite's worker plugin would otherwise emit — are never
// produced. The anchors must guard on the raw `__STUDIO__` define, not the
// imported `IS_STUDIO` const: an imported binding only tree-shakes after
// transform, too late to stop the worker emit (see profile.ts).
const Projects = __STUDIO__ ? lazy(() => import("./views/Projects")) : null;
const ProjectWorkspace = __STUDIO__
  ? lazy(() => import("./views/ProjectWorkspace"))
  : null;
const Extensions = __STUDIO__ ? lazy(() => import("./views/Extensions")) : null;
// Operator surface — always present in both profiles.
const Workers = lazy(() => import("./views/Workers"));
const Metrics = lazy(() => import("./views/Metrics"));
const Traces = lazy(() => import("./views/Traces"));
const Explorer = lazy(() => import("./views/Explorer"));
const Config = lazy(() => import("./views/Config"));
const Credits = lazy(() => import("./views/Credits"));

// 24×24 stroke icons, drawn to lucide-style metrics so the rail reads as one
// family.
function Icon({ d, children }: { d?: string; children?: ReactNode }) {
  return (
    <svg
      className="h-4 w-4 shrink-0"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {d ? <path d={d} /> : children}
    </svg>
  );
}

const icons = {
  projects: (
    <Icon d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" />
  ),
  extensions: (
    <Icon d="M14 4a2 2 0 1 1 4 0v2h2a2 2 0 0 1 2 2v3h-2a2.5 2.5 0 0 0 0 5h2v3a2 2 0 0 1-2 2h-3v-2a2.5 2.5 0 0 0-5 0v2H9a2 2 0 0 1-2-2v-3H5a2.5 2.5 0 0 1 0-5h2V8a2 2 0 0 1 2-2h5z" />
  ),
  topology: (
    <Icon>
      <circle cx="12" cy="5" r="2.5" />
      <circle cx="5" cy="19" r="2.5" />
      <circle cx="19" cy="19" r="2.5" />
      <path d="M12 7.5 6.3 17M12 7.5l5.7 9.5M7.5 19h9" />
    </Icon>
  ),
  metrics: <Icon d="M3 21h18M7 16v-5M12 16V8M17 16v-8" />,
  explorer: (
    <Icon>
      <circle cx="11" cy="11" r="7" />
      <path d="m21 21-4.3-4.3" />
    </Icon>
  ),
  traces: <Icon d="M22 12h-4l-3 8L9 4l-3 8H2" />,
  workers: (
    <Icon>
      <rect x="4" y="4" width="16" height="16" rx="2" />
      <rect x="9" y="9" width="6" height="6" />
      <path d="M9 1v3M15 1v3M9 20v3M15 20v3M1 9h3M1 15h3M20 9h3M20 15h3" />
    </Icon>
  ),
  docs: (
    <Icon>
      <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
      <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z" />
    </Icon>
  ),
  whitepaper: (
    <Icon>
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <path d="M14 2v6h6M16 13H8M16 17H8M10 9H8" />
    </Icon>
  ),
  credits: (
    <Icon>
      <rect x="2" y="4" width="20" height="16" rx="2" />
      <path d="M7 4v16M17 4v16M2 8h5M2 12h5M2 16h5M17 8h5M17 12h5M17 16h5" />
    </Icon>
  ),
  feedback: (
    <Icon>
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </Icon>
  ),
  config: (
    <Icon>
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </Icon>
  ),
  sun: (
    <Icon>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </Icon>
  ),
  moon: <Icon d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" />,
  system: (
    <Icon>
      <rect x="2" y="3" width="20" height="14" rx="2" />
      <path d="M8 21h8M12 17v4" />
    </Icon>
  ),
} as const;

const navItems: {
  to: string;
  label: string;
  icon: ReactNode;
  studio?: boolean;
}[] = [
  { to: "/projects", label: "Projects", icon: icons.projects, studio: true },
  {
    to: "/extensions",
    label: "Extensions",
    icon: icons.extensions,
    studio: true,
  },
  { to: "/topology", label: "Topology", icon: icons.topology },
  { to: "/metrics", label: "Metrics", icon: icons.metrics },
  { to: "/explorer", label: "Explorer", icon: icons.explorer },
  { to: "/traces", label: "Traces", icon: icons.traces },
  { to: "/workers", label: "Workers", icon: icons.workers },
].filter((i) => IS_STUDIO || !i.studio);

// Where "home" lands: the maker starts in Projects; the operator ("observe"
// build, no Projects route) starts on Topology.
const HOME_ROUTE = IS_STUDIO ? "/projects" : "/topology";

function railItemClass(active: boolean): string {
  return `relative flex items-center gap-2.5 rounded-md px-3 py-2 text-sm no-underline transition-colors ${
    active
      ? "bg-accent/10 font-medium text-accent-strong"
      : "text-fg-muted hover:bg-hover hover:text-fg"
  }`;
}

/** Accent bar marking the active rail item. */
function ActiveBar({ show }: { show: boolean }) {
  if (!show) return null;
  return (
    <span className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-gradient-to-b from-accent to-accent-2" />
  );
}

/** Sidebar segmented control cycling the appearance: light / dark / system.
 * Theme packs and imports are picked in Config → Appearance. */
function ThemeToggle() {
  const { selection, select } = useTheme();
  const modes = [
    { mode: "light", icon: icons.sun, title: "Light" },
    { mode: "dark", icon: icons.moon, title: "Dark" },
    { mode: "system", icon: icons.system, title: "Follow system" },
  ] as const;
  return (
    <div className="mx-3 mb-3 flex rounded-lg border border-edge bg-inset p-0.5">
      {modes.map((m) => {
        const active = selection.mode === m.mode;
        return (
          <button
            key={m.mode}
            title={
              selection.mode === "theme"
                ? `${m.title} (a theme pack is active — this switches back)`
                : m.title
            }
            onClick={() => select({ mode: m.mode })}
            className={`flex flex-1 items-center justify-center rounded-md py-1.5 transition-colors ${
              active
                ? "bg-raised text-accent-strong shadow-sm"
                : "text-fg-faint hover:text-fg"
            }`}
          >
            {m.icon}
          </button>
        );
      })}
    </div>
  );
}

/**
 * Delay before the startup persona panel (#464) opens, letting the initial
 * route render and the journey list settle before the modal appears.
 */
const STARTUP_PANEL_DELAY_MS = 500;

export default function App() {
  const location = useLocation();
  // The one product-tour instance for the whole app. Published via TourContext
  // so the rail button here AND the empty-state journey pickers (#411) drive the
  // same runner and journey state.
  //
  // `autoStart` is now false: ADR 0049 replaces first-run auto-start with the
  // journey picker on the Projects/Topology empty state (#411, landed), so a
  // first-timer chooses one of the real outcome-shaped journeys instead of being
  // dropped into the demoted overview. The overview stays reachable from each
  // picker's "just show me around" link and the rail's "Take a tour".
  const tour = useProductTour({ autoStart: false });
  const { startTour, resumeJourney, activeJourney, isRunning } = tour;
  // Offer "Resume" only when there is an unfinished journey that is not already
  // on screen — otherwise the label would invite the user to resume the tour
  // they are looking at.
  const canResume = !!activeJourney && !isRunning;

  // The startup persona panel (#464): the front door that replaces the CLI's
  // `?tour=` link-spray. Personas are the offerable, outcome-shaped journeys
  // (everything but the zero-commitment overview), derived from the registry.
  const personaJourneys = pickerJourneys(
    tour.availableJourneys,
    CONSOLE_PROFILE,
  );
  const [startupOpen, setStartupOpen] = useState(false);
  // One-shot decision, deferred a beat so the initial route renders first and
  // the journey list settles against real context. Never interrupts a journey
  // already running or resumable (e.g. a `?tour=` deep link that still fires).
  const startupDecided = useRef(false);
  const personaJourneysRef = useRef(personaJourneys);
  personaJourneysRef.current = personaJourneys;
  useEffect(() => {
    if (startupDecided.current) return;
    if (!tour.showStartupPanel) {
      startupDecided.current = true;
      return;
    }
    const id = window.setTimeout(() => {
      startupDecided.current = true;
      if (isRunning || activeJourney) return;
      if (personaJourneysRef.current.length === 0) return;
      setStartupOpen(true);
    }, STARTUP_PANEL_DELAY_MS);
    return () => window.clearTimeout(id);
  }, [tour.showStartupPanel, isRunning, activeJourney]);
  // Remember the last place the user was within the Projects section (the
  // project list or a specific workspace) so the rail's "Projects" item returns
  // them there after a detour through Metrics/Traces/etc. — instead of always
  // dropping back at the root list.
  const projectsRoute = useRef(
    localStorage.getItem("nano.projectsRoute") || "/projects",
  );
  useEffect(() => {
    if (location.pathname.startsWith("/projects")) {
      projectsRoute.current = location.pathname;
      localStorage.setItem("nano.projectsRoute", location.pathname);
    }
  }, [location.pathname]);

  // The running gateway's version, shown in the sidebar chrome so it's visible
  // on every page — useful when bouncing between dev builds and staged releases
  // to confirm which binary is actually serving the console. Reads
  // `/console/api/topology`'s `gateway_version` (the same field surfaced on the
  // Topology page); silently absent if the probe fails.
  const [serverVersion, setServerVersion] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    getTopology({ throwOnError: true })
      .then(({ data }) => {
        if (!cancelled) setServerVersion(data.gateway_version);
      })
      .catch(() => {
        /* leave hidden — sidebar is not the place to surface a probe error */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // "What's new" changelog. The document is a static asset generated at build
  // time from the git history (console/scripts/build-changelog.mjs) and served
  // at `${BASE_URL}changelog.json`; we fetch it once and keep an unobtrusive
  // "new" dot on the version chrome until the user opens the panel. The last
  // acknowledged version is persisted so the dot only reappears after a genuine
  // upgrade. Offline-soft: a missing/failed asset simply hides the affordance's
  // badge and shows a graceful message if the panel is opened.
  const [changelog, setChangelog] = useState<ChangelogDoc | null>(null);
  const [changelogError, setChangelogError] = useState(false);
  const [changelogOpen, setChangelogOpen] = useState(false);
  const [lastSeenChangelog, setLastSeenChangelog] = useState<string | null>(
    () => localStorage.getItem("nano.changelog.lastSeen"),
  );
  useEffect(() => {
    let cancelled = false;
    fetch(`${import.meta.env.BASE_URL}changelog.json`, {
      headers: { accept: "application/json" },
    })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("not ok"))))
      .then((doc: ChangelogDoc) => {
        if (!cancelled) setChangelog(doc);
      })
      .catch(() => {
        if (!cancelled) setChangelogError(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const changelogHasUnseen = hasUnseenSince(changelog, lastSeenChangelog);
  const openChangelog = () => setChangelogOpen(true);
  const closeChangelog = () => {
    setChangelogOpen(false);
    // Closing acknowledges the newest version, clearing the dot. Persist on
    // close (not open) so opening before changelog.json has loaded still marks
    // the release seen once it arrives, and update state so the dot clears
    // without a reload.
    const newest = changelog?.versions[0]?.version;
    if (newest) {
      localStorage.setItem("nano.changelog.lastSeen", newest);
      setLastSeenChangelog(newest);
    }
  };

  // Marketplace update poll (30s cadence) so the Extensions rail item can wear
  // a badge with the current available-updates count on every page — the user
  // doesn't have to open Extensions to notice a freshly-published fix. Skipped
  // while the tab is hidden (background tabs shouldn't hammer npm). The
  // server-side marketplace() also probes `npm view` per installed pack so
  // this is not gated by npm's search-index lag.
  const [updateCount, setUpdateCount] = useState(0);
  useEffect(() => {
    // Studio-only: the operator ("observe") build has no Extensions view, so
    // there's no badge to feed and no reason to poll npm.
    if (!IS_STUDIO) return;
    let cancelled = false;
    const poll = () => {
      if (typeof document !== "undefined" && document.hidden) return;
      getMarketplace({ throwOnError: true })
        .then(({ data }) => {
          if (cancelled) return;
          setUpdateCount(data.entries.filter((e) => e.updateAvailable).length);
        })
        .catch(() => {
          /* offline or npm missing — leave the badge as-is */
        });
    };
    poll();
    const id = window.setInterval(poll, 30_000);
    const onVis = () => {
      if (!document.hidden) poll();
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      cancelled = true;
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, []);

  // Register every installed lang pack's `fileTypes[]` with the Monaco
  // ext→language map at boot, and again whenever the user navigates — so a
  // pack installed via the Extensions view during this session takes effect
  // as soon as they open a project, without a hard reload. Extensions.tsx
  // also calls the same helper right after install/remove, which is the
  // fast path; this navigation-triggered refetch is the safety net for any
  // other codepath that might mutate the extension set.
  useEffect(() => {
    // The ext→language + IntelliSense maps only matter to the Monaco editors,
    // which the operator ("observe") build doesn't ship.
    if (!IS_STUDIO) return;
    getExtensions({ throwOnError: true })
      .then(({ data }) => {
        registerFileTypesFromOverview(data);
        setIntellisenseFromOverview(data);
      })
      .catch(() => {
        /* ignore — the static fallback table still covers the common cases */
      });
  }, [location.pathname]);

  return (
    <TourContext.Provider value={tour}>
      <div className="flex h-full bg-app text-fg">
        <aside className="flex w-56 shrink-0 flex-col border-r border-edge bg-panel">
          <div className="px-5 py-4">
            <a href="/" className="block no-underline">
              <div className="bg-gradient-to-r from-accent to-accent-2 bg-clip-text text-lg font-bold tracking-tight text-transparent">
                nano BPM
              </div>
              <div className="text-xs text-fg-faint">single-node console</div>
              <div className="mt-2 inline-block rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent-strong">
                Advanced Research Prototype
              </div>
              <div className="mt-1.5 text-[10px] text-fg-faint">
                Free for evaluation use
              </div>
            </a>
            {/* Version chrome doubles as the "What's new" entry point. It sits
                outside the home <a> (a button can't nest in an anchor) and wears
                a dot until the newest release has been opened. */}
            {(serverVersion || changelog || changelogError) && (
              <button
                type="button"
                onClick={openChangelog}
                title="See what's new in Nano"
                className="mt-1 flex items-center gap-1.5 rounded font-mono text-[10px] text-fg-faint outline-none transition-colors hover:text-fg focus-visible:ring-2 focus-visible:ring-accent"
              >
                <span>
                  {serverVersion
                    ? `gateway v${normalizeVersion(serverVersion) ?? serverVersion}`
                    : "What's new"}
                </span>
                {serverVersion && (
                  <span className="underline decoration-dotted underline-offset-2">
                    What's new
                  </span>
                )}
                {changelogHasUnseen && (
                  <>
                    <span className="sr-only">New changes available</span>
                    <span
                      className="inline-block h-1.5 w-1.5 rounded-full bg-accent"
                      aria-hidden="true"
                    />
                  </>
                )}
              </button>
            )}
          </div>
          <nav className="flex flex-col gap-1 px-3">
            {navItems.map((item) => {
              // The Projects item is special: it links back to wherever the user
              // last was in that section and stays highlighted across all
              // /projects/* routes.
              const isProjects = item.to === "/projects";
              const to = isProjects ? projectsRoute.current : item.to;
              const active = isProjects
                ? location.pathname.startsWith("/projects")
                : location.pathname === item.to;
              return (
                <NavLink
                  key={item.to}
                  to={to}
                  data-tour={navAnchor(item.to)}
                  className={railItemClass(active)}
                >
                  <ActiveBar show={active} />
                  {item.icon}
                  {item.label}
                  {item.to === "/extensions" && updateCount > 0 && (
                    <span
                      className="ml-auto inline-flex min-w-[18px] items-center justify-center rounded-full bg-danger px-1.5 text-[10px] font-bold leading-none text-white"
                      style={{ height: "18px" }}
                      title={`${updateCount} extension update${updateCount === 1 ? "" : "s"} available`}
                      aria-label={`${updateCount} extension updates available`}
                    >
                      {updateCount > 99 ? "99+" : updateCount}
                    </span>
                  )}
                </NavLink>
              );
            })}
          </nav>

          <button
            type="button"
            onClick={canResume ? resumeJourney : startTour}
            data-tour={TOUR_ANCHOR.takeATour}
            className={`mt-auto mx-3 ${railItemClass(false)}`}
            title={
              canResume
                ? `Pick up “${activeJourney.title}” where you left off`
                : "Replay the product tour"
            }
          >
            <Icon>
              <circle cx="12" cy="12" r="9" />
              <path d="M9.1 9a3 3 0 1 1 4.3 3.2c-.8.5-1.4 1-1.4 1.9" />
              <path d="M12 17h.01" />
            </Icon>
            {canResume ? "Resume tour" : "Take a tour"}
          </button>

          <a
            href="https://github.com/jwulf/nano-ide/issues/new/choose"
            target="_blank"
            rel="noopener noreferrer"
            className={`mx-3 ${railItemClass(false)}`}
            title="Send feedback or report an issue"
          >
            {icons.feedback}
            Feedback
          </a>

          <a
            href="/docs"
            className={`mx-3 ${railItemClass(false)}`}
            title="Documentation"
          >
            {icons.docs}
            Documentation
          </a>

          <a
            href="/whitepaper"
            className={`mx-3 ${railItemClass(false)}`}
            title="Whitepaper"
          >
            {icons.whitepaper}
            Whitepaper
          </a>

          <NavLink
            to="/credits"
            className={`mx-3 ${railItemClass(location.pathname.startsWith("/credits"))}`}
            title="Credits"
          >
            <ActiveBar show={location.pathname.startsWith("/credits")} />
            {icons.credits}
            Credits
          </NavLink>

          <NavLink
            to="/config"
            className={`mx-3 mb-3 ${railItemClass(location.pathname.startsWith("/config"))}`}
            title="Configuration"
          >
            <ActiveBar show={location.pathname.startsWith("/config")} />
            {icons.config}
            Config
          </NavLink>

          <ThemeToggle />
        </aside>

        <main className="min-w-0 flex-1 overflow-auto">
          <Suspense
            fallback={
              <div className="flex h-full items-center justify-center text-sm text-fg-faint">
                Loading…
              </div>
            }
          >
            <RouteErrorBoundary resetKey={location.pathname}>
              <Routes>
                <Route
                  path="/"
                  element={<Navigate to={HOME_ROUTE} replace />}
                />
                {/* Studio-only routes — absent (and tree-shaken) in observe builds.
                  RR6 ignores falsy children, so a null component drops the route. */}
                {Projects && <Route path="/projects" element={<Projects />} />}
                {ProjectWorkspace && (
                  <Route
                    path="/projects/:name"
                    element={<ProjectWorkspace />}
                  />
                )}
                {Extensions && (
                  <Route path="/extensions" element={<Extensions />} />
                )}
                <Route path="/config" element={<Config />} />
                <Route path="/credits" element={<Credits />} />
                <Route path="/topology" element={<Topology />} />
                <Route path="/metrics" element={<Metrics />} />
                <Route
                  path="/modeler"
                  element={<Navigate to={HOME_ROUTE} replace />}
                />
                <Route path="/explorer" element={<Explorer />} />
                <Route path="/traces" element={<Traces />} />
                <Route path="/workers" element={<Workers />} />
                <Route
                  path="*"
                  element={<Navigate to={HOME_ROUTE} replace />}
                />
              </Routes>
            </RouteErrorBoundary>
          </Suspense>
        </main>
      </div>
      {startupOpen && (
        <StartupJourneyPanel
          journeys={personaJourneys}
          showAtStartup={tour.showStartupPanel}
          onToggleShowAtStartup={tour.setShowStartupPanel}
          onPick={(journeyId) => {
            setStartupOpen(false);
            tour.startJourney(journeyId);
          }}
          onOverview={() => {
            setStartupOpen(false);
            startTour();
          }}
          onClose={() => setStartupOpen(false)}
        />
      )}
      {changelogOpen && (
        <ChangelogPanel
          doc={changelog}
          loadError={changelogError}
          onClose={closeChangelog}
        />
      )}
    </TourContext.Provider>
  );
}
