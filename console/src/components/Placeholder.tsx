interface PlaceholderProps {
  title: string;
  blurb: string;
  planned: string[];
}

// A consistent "coming soon" panel for console views that are scaffolded but not
// yet implemented. Keeps the navigation/layout real while the feature is built.
export default function Placeholder({ title, blurb, planned }: PlaceholderProps) {
  return (
    <div className="p-8">
      <header className="mb-6">
        <h1 className="text-2xl font-semibold text-fg">{title}</h1>
        <p className="text-sm text-fg-muted">{blurb}</p>
      </header>
      <div className="max-w-lg rounded-lg border border-dashed border-edge-strong bg-raised p-6">
        <div className="mb-3 text-xs font-medium uppercase tracking-wide text-fg-faint">
          Planned
        </div>
        <ul className="space-y-2 text-sm text-fg-muted">
          {planned.map((item) => (
            <li key={item} className="flex items-start gap-2">
              <span className="mt-1 h-1.5 w-1.5 shrink-0 rounded-full bg-fg-faint" />
              {item}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
