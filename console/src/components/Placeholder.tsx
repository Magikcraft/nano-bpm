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
        <h1 className="text-2xl font-semibold">{title}</h1>
        <p className="text-sm text-zinc-500">{blurb}</p>
      </header>
      <div className="max-w-lg rounded-lg border border-dashed border-zinc-700 bg-zinc-900/50 p-6">
        <div className="mb-3 text-xs font-medium uppercase tracking-wide text-zinc-500">
          Planned
        </div>
        <ul className="space-y-2 text-sm text-zinc-300">
          {planned.map((item) => (
            <li key={item} className="flex items-start gap-2">
              <span className="mt-1 h-1.5 w-1.5 shrink-0 rounded-full bg-zinc-600" />
              {item}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
