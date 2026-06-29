import { useMemo } from "react";
import MarkdownIt from "markdown-it";

// `html: false` keeps raw HTML in the source escaped, so rendering untrusted
// markdown from a project file cannot inject scripts.
const md = new MarkdownIt({
  html: false,
  linkify: true,
  typographer: true,
});

/// Renders markdown source to styled HTML for the IDE's `.md` preview tab.
export default function MarkdownPreview({ source }: { source: string }) {
  const html = useMemo(() => md.render(source), [source]);
  return (
    <div className="h-full overflow-auto bg-zinc-950 px-6 py-5">
      <div
        className="markdown-preview mx-auto max-w-3xl text-sm leading-relaxed text-zinc-200"
        // Safe: markdown-it is configured with html:false, so any raw HTML in
        // the source is escaped rather than injected.
        dangerouslySetInnerHTML={{ __html: html }}
      />
    </div>
  );
}
