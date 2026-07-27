import type {
  ButtonHTMLAttributes,
  InputHTMLAttributes,
  ReactNode,
} from "react";

// Shared UI primitives — the building blocks every view composes so the
// console reads as one product. All colours come from theme tokens
// (src/theme/tokens.css); never hardcode a palette colour in a view.

/** Standard page chrome: title, one-line subtitle, optional right-side actions. */
export function PageHeader({
  title,
  subtitle,
  actions,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <header className="mb-6 flex flex-wrap items-start justify-between gap-3">
      <div>
        <h1 className="text-xl font-semibold tracking-tight text-fg">
          {title}
        </h1>
        {subtitle && (
          <p className="mt-1 max-w-2xl text-sm text-fg-muted">{subtitle}</p>
        )}
      </div>
      {actions && (
        <div className="flex shrink-0 items-center gap-2">{actions}</div>
      )}
    </header>
  );
}

/** Uppercase section label above a group of cards/rows. */
export function SectionLabel({ children }: { children: ReactNode }) {
  return (
    <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-fg-faint">
      {children}
    </h2>
  );
}

export function Card({
  className = "",
  children,
  ...rest
}: { className?: string; children: ReactNode } & Record<string, unknown>) {
  return (
    <div
      className={`rounded-xl border border-edge bg-raised shadow-sm ${className}`}
      {...rest}
    >
      {children}
    </div>
  );
}

type ButtonVariant = "primary" | "secondary" | "danger" | "ghost";
type ButtonSize = "sm" | "md";

const buttonVariants: Record<ButtonVariant, string> = {
  primary:
    "bg-accent text-on-accent hover:bg-accent-strong border border-transparent shadow-sm",
  secondary:
    "border border-edge-strong bg-raised text-fg hover:bg-hover shadow-sm",
  danger: "bg-danger/10 text-danger border border-danger/30 hover:bg-danger/20",
  ghost: "border border-transparent text-fg-muted hover:bg-hover hover:text-fg",
};

const buttonSizes: Record<ButtonSize, string> = {
  sm: "px-3 py-1.5 text-xs",
  md: "px-4 py-2 text-sm",
};

export function Button({
  variant = "secondary",
  size = "md",
  className = "",
  ...rest
}: {
  variant?: ButtonVariant;
  size?: ButtonSize;
} & ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      className={`inline-flex items-center justify-center gap-1.5 rounded-md font-medium transition-colors outline-none focus-visible:ring-2 focus-visible:ring-accent/60 disabled:cursor-not-allowed disabled:opacity-50 ${buttonVariants[variant]} ${buttonSizes[size]} ${className}`}
      {...rest}
    />
  );
}

type BadgeTone = "neutral" | "accent" | "ok" | "warn" | "danger" | "info";

const badgeTones: Record<BadgeTone, string> = {
  neutral: "bg-hover text-fg-muted",
  accent: "bg-accent/10 text-accent-strong border border-accent/30",
  ok: "bg-ok/10 text-ok",
  warn: "bg-warn/10 text-warn",
  danger: "bg-danger/10 text-danger",
  info: "bg-info/10 text-info",
};

export function Badge({
  tone = "neutral",
  className = "",
  children,
}: {
  tone?: BadgeTone;
  className?: string;
  children: ReactNode;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${badgeTones[tone]} ${className}`}
    >
      {children}
    </span>
  );
}

/** Shared text-input styling (also exported as a class string for selects /
 * textareas that can't use the component). */
export const inputClass =
  "rounded-md border border-edge bg-inset px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none transition-colors focus:border-accent focus-visible:ring-2 focus-visible:ring-accent/40";

export function Input({
  className = "",
  ...rest
}: InputHTMLAttributes<HTMLInputElement>) {
  return <input className={`${inputClass} ${className}`} {...rest} />;
}

export function EmptyState({
  title,
  hint,
  action,
}: {
  title: ReactNode;
  hint?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-xl border border-dashed border-edge-strong px-6 py-12 text-center">
      <div className="text-sm font-medium text-fg-muted">{title}</div>
      {hint && <div className="max-w-md text-xs text-fg-faint">{hint}</div>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

/** Inline error line under a form/action. */
export function ErrorText({ children }: { children: ReactNode }) {
  return <div className="text-sm text-danger">{children}</div>;
}
