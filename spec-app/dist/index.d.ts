/** A model file to index. `kind` selects the parser; `text` is the file body. */
export interface ModelFile {
	path: string;
	kind: "bpmn" | "dmn" | "form";
	text: string;
}
export interface UserTaskSymbol {
	id: string;
	name?: string;
	/** zeebe:formDefinition formId, if the task references a form. */
	formId?: string;
}
export interface ProcessSymbol {
	id: string;
	name?: string;
	executable: boolean;
	/** Message names on message start events (targets of action.message that start an instance). */
	messageStartEvents: string[];
	userTasks: UserTaskSymbol[];
	/** zeebe:taskDefinition types of service tasks (worker taskTypes). */
	serviceTaskTypes: string[];
}
export interface DecisionSymbol {
	id: string;
	name?: string;
}
export interface FormFieldSymbol {
	key: string;
	type: string;
}
export interface FormSymbol {
	id: string;
	fields: FormFieldSymbol[];
}
/** The primitive field types a domain type (ADR 0029 §4 / ADR 0031) may use. */
export type DomainPrimitive = "string" | "number" | "integer" | "boolean" | "date" | "datetime" | "json";
export declare const DOMAIN_PRIMITIVES: readonly DomainPrimitive[];
export interface InferredField {
	key: string;
	type: DomainPrimitive;
}
/**
 * A candidate domain record inferred from a form's fields — the ADR 0029 §4
 * on-ramp: the maker either promotes it into the `types` registry or binds it
 * to a datasource table. Inference is heuristic (form keys are free strings),
 * so it is a suggestion, never a silently-invented schema.
 */
export interface InferredRecord {
	/** Candidate type id — the source form's id. */
	id: string;
	source: "form";
	sourcePath: string;
	fields: InferredField[];
}
export interface SymbolIndex {
	processes: ProcessSymbol[];
	/** All declared bpmn:message names (targets of action.message). */
	messages: string[];
	decisions: DecisionSymbol[];
	forms: FormSymbol[];
	/** Candidate domain records inferred from forms (ADR 0029 §4 promotion on-ramp). */
	inferredRecords: InferredRecord[];
	/** Non-fatal problems encountered while parsing a model file. */
	parseErrors: {
		path: string;
		message: string;
	}[];
}
/**
 * Map a form-js component `type` to a domain primitive (ADR 0029 §4). Heuristic
 * and deliberately conservative — anything not clearly numeric/boolean/temporal
 * falls back to `string`, and the maker confirms on promotion.
 */
export declare function formTypeToPrimitive(formType: string): DomainPrimitive;
/**
 * Build the symbol index from a project's model files. Parse failures are
 * collected in `parseErrors` rather than thrown, so one malformed model does not
 * blind the index to the rest of the project.
 */
export declare function buildSymbolIndex(models: ModelFile[]): Promise<SymbolIndex>;
/** Classify a model file by extension (helper for callers listing a project dir). */
export declare function modelKindOf(path: string): ModelFile["kind"] | undefined;
export interface ResolvedField {
	key: string;
	/** A primitive, or the id of another declared type (nominal). */
	type: string;
	optional: boolean;
	list: boolean;
}
export interface ResolvedDomainType {
	id: string;
	name?: string;
	/** Identity discipline; "nominal" today (the structural escape hatch is reserved). */
	match: "nominal" | "structural";
	/** Datasource table this type binds to as its rest projection, if any. */
	table?: string;
	fields: ResolvedField[];
}
export interface DomainTypeResolution {
	/** Types declared in the manifest `types` registry. */
	declared: ResolvedDomainType[];
	/** Form-inferred candidates not already declared — a maker may promote these. */
	inferred: InferredRecord[];
}
/**
 * Resolve the domain types a maker can reference. Pass the project `index` to
 * include form-inferred candidates; omit it for the declared registry alone.
 */
export declare function resolveDomainTypes(manifest: unknown, index?: SymbolIndex): DomainTypeResolution;
/** The kinds of reference a manifest string value can be. */
export type ReferenceSite = "process" | "message" | "decision" | "field-type" | "body-type" | "datasource" | "agent";
export type CandidateKind = "process" | "message" | "decision" | "primitive" | "type" | "datasource" | "agent" | "variable";
export interface CompletionCandidate {
	/** The literal id/name to insert (unquoted). */
	value: string;
	kind: CandidateKind;
	/** Short human hint (e.g. a process name or "primitive"). */
	detail?: string;
}
export interface ManifestCompletion {
	site: ReferenceSite | "feel";
	/** Offset span of the string *content* (between the quotes) to replace. */
	range: {
		start: number;
		end: number;
	};
	candidates: CompletionCandidate[];
}
/** Just the parts of the index this engine reads (keeps callers flexible). */
export type CompletionIndex = Pick<SymbolIndex, "processes" | "messages" | "decisions">;
/**
 * The public entry point: what completions apply at `offset`, or null when the
 * cursor is not inside a recognized reference value or FEEL expression.
 */
export declare function manifestCompletionAt(text: string, offset: number, manifest: unknown, index?: CompletionIndex): ManifestCompletion | null;
export interface Diagnostic {
	severity: "error";
	/** JSON Pointer (RFC 6901) to the offending node. */
	pointer: string;
	message: string;
	/** Stable code: "schema" for shape errors, else the cross-reference rule id. */
	code: string;
}
export interface ValidationResult {
	ok: boolean;
	diagnostics: Diagnostic[];
}
/**
 * Validate a manifest fail-closed. Pass the project `index` to enable the
 * model-resolving rules (start/message/decision); omit it for a manifest-only
 * lint (schema + intra-manifest references). Returns `ok:false` with the
 * diagnostics whenever anything fails.
 */
export declare function validateManifest(manifest: unknown, index?: SymbolIndex): ValidationResult;
type GlobList = string[];
type GlobList1 = string[];
type GlobList2 = string[];
type EnvTemplate = string;
type Slug = string;
type TriggerAction = {
	/**
	 * Process id/name to start; variables seeded by a FEEL expression over the event body.
	 */
	start?: string;
	/**
	 * FEEL over the event body producing the started instance's variables.
	 */
	variables?: string;
	/**
	 * messageName to publish as a CorrelateMessage (correlates to a message-start subscription to start a new instance, or to a running-instance catch to feed a token — ADR 0025 §5).
	 */
	message?: string;
	/**
	 * FEEL over the event body producing the correlationKey (message actions).
	 */
	correlationKey?: string;
} & TriggerAction1;
type TriggerAction1 = {
	[k: string]: unknown;
};
type Worker = {
	taskType: string;
	/**
	 * Path to a handler file (language via ADR 0008 packs).
	 */
	handler?: string;
	/**
	 * Name of an llm[] binding used as the worker (LLM-as-worker).
	 */
	llm?: string;
} & Worker1;
type Worker1 = {
	[k: string]: unknown;
};
type SecurityMode = "none" | "local" | "oidc";
/**
 * Urban App manifest (nano.app.json) — the declared-data binding of an Urban RAD application (ADR 0027). Owns the envelope + cross-reference rules; each block's detail is owned by its ADR (data=0024, triggers=0025, surfaces=0026, security=0028, workers/llm=0022 §E). This file is the source of truth: TypeScript types are generated from it (scripts/generate-app-manifest.sh) and it doubles as the $schema editors use for nano.app.json autocompletion.
 */
export interface AppManifest {
	/**
	 * Optional editor hint pointing at this schema for autocompletion.
	 */
	$schema?: string;
	/**
	 * Manifest schema version for forward-compat. Currently always 1.
	 */
	schemaVersion: 1;
	/**
	 * Stable App identifier (slug). Required.
	 */
	id: string;
	/**
	 * Human-readable App name. Required.
	 */
	name: string;
	/**
	 * Informational codename, surfaced as App.CODENAME (ADR 0015). Optional.
	 */
	codename?: string;
	runtime?: Runtime;
	models?: Models;
	data?: Data;
	/**
	 * The domain type registry (ADR 0029 §4, ADR 0031). Named record types keyed by a stable id — the *nominal* identity every reference resolves against. A type's fields project onto three shapes: form field (face), process variable (motion) and datasource row (rest); the Process-Relational Mapper (ADR 0031) generates the mapping. Types here are the transient/declared source; a datasource table is the other (ADR 0029 §4).
	 */
	types?: {
		[k: string]: DomainType;
	};
	/**
	 * Event sources bound to engine actions (ADR 0025).
	 */
	triggers?: Trigger[];
	/**
	 * Named connections (credentials/endpoint) referenced by triggers/workers by id, so configs carry no inline secrets (ADR 0025 §1).
	 */
	connections?: {
		[k: string]: Connection;
	};
	surfaces?: Surfaces;
	/**
	 * Service-task handlers: referenced files or an llm binding (ADR 0022 §E).
	 */
	workers?: Worker[];
	/**
	 * Named LLM bindings usable as workers or as a chat surface agent (ADR 0022 §E).
	 */
	llm?: {
		[k: string]: LlmBinding;
	};
	security?: Security;
}
interface Runtime {
	/**
	 * How the App reaches the engine at runtime.
	 */
	engine?: "embedded" | "remote" | "cluster";
	node?: "single" | "cluster";
}
interface Models {
	processes?: GlobList;
	decisions?: GlobList1;
	forms?: GlobList2;
}
interface Data {
	/**
	 * Name of the datasource used when a consumer names none.
	 */
	default?: string;
	sources: {
		[k: string]: DataSource;
	};
}
interface DataSource {
	/**
	 * Driver id. May be an env template so deployment flips SQLite to Postgres without a source change (ADR 0024 §1).
	 */
	driver: ("sqlite" | "postgres") | EnvTemplate;
	/**
	 * Connection URL, typically an env template (e.g. file:./app.db or ${NANO_APP_DB_URL:-file:./app.db}).
	 */
	url: string;
	/**
	 * Path to a migrations directory.
	 */
	migrations?: string;
}
interface DomainType {
	/**
	 * Human-readable label. The map key remains the stable id every reference uses.
	 */
	name?: string;
	/**
	 * Identity discipline. `nominal` (default): references resolve by this type's id. `structural` is a reserved escape hatch (match by field shape) — declared here but not yet honoured by the validator/mapper.
	 */
	match?: "nominal" | "structural";
	/**
	 * Optional datasource table this type binds to as its rest projection (ADR 0031 rest bank). Absent = transient / non-persisted (ADR 0029 §4.2). Table existence is validated once the datasource schema() runtime (ADR 0024) lands; the shape is checked now.
	 */
	table?: string;
	/**
	 * Field name → field definition. Field names are the keys the form field, the variable path and the datasource column share (ADR 0029 §4).
	 */
	fields: {
		[k: string]: DomainField;
	};
}
interface DomainField {
	/**
	 * A primitive type, or the id of another domain type in the registry (nominal reference). Primitive ids take precedence over an identically named type.
	 */
	type: ("string" | "number" | "integer" | "boolean" | "date" | "datetime" | "json") | Slug;
	/**
	 * Whether the field may be absent.
	 */
	optional?: boolean;
	/**
	 * Whether the field is a list of `type` rather than a single value.
	 */
	list?: boolean;
}
interface Trigger {
	id: Slug;
	/**
	 * Source kind. Core (in-binary): cron | webhook | file. Pack sources add imap, mqtt, cloud, … (ADR 0025 §1).
	 */
	type: string;
	/**
	 * cron: the crontab spec (e.g. '0 6 * * *').
	 */
	spec?: string;
	/**
	 * webhook: the HTTP path served on the App backend (e.g. /hooks/temp).
	 */
	path?: string;
	/**
	 * Name of a connections[] entry supplying this source's credentials (e.g. imap mailbox).
	 */
	connection?: string;
	/**
	 * Inbound auth policy for a webhook, e.g. 'hmac:sensors' referencing a connection (ADR 0025).
	 */
	auth?: string;
	/**
	 * Lowercase kebab-case slug.
	 */
	bodyType?: string;
	action: TriggerAction;
}
interface Connection {
	/**
	 * Connection kind (e.g. imap, mqtt, hmac).
	 */
	type: string;
}
interface Surfaces {
	taskInbox?: TaskInboxSurface;
	chat?: ChatSurface;
}
interface TaskInboxSurface {
	enabled?: boolean;
	path?: string;
}
interface ChatSurface {
	enabled?: boolean;
	path?: string;
	/**
	 * Name of an llm[] binding backing this chat (cross-reference rule, ADR 0027 §4).
	 */
	agent?: string;
}
interface LlmBinding {
	/**
	 * LLM provider selector (e.g. 'env' to resolve from environment).
	 */
	provider: string;
	/**
	 * Model id, typically an env template (e.g. ${NANO_APP_LLM_MODEL}).
	 */
	model: string;
	/**
	 * Constrains the model's structured output (e.g. to a DMN decision).
	 */
	output?: {
		/**
		 * DMN decision id constraining the output shape.
		 */
		decision?: string;
	};
	/**
	 * Action-API tools the agent may call (e.g. start-process, complete-task, query-data).
	 */
	tools?: string[];
}
interface Security {
	/**
	 * Enabled auth tier(s): none (default), local (username/password), oidc (social). A list combines them.
	 */
	mode?: SecurityMode | SecurityMode[];
	providers?: SecurityProvider[];
	/**
	 * Role names; the maker may add domain roles beyond admin/user.
	 */
	roles?: string[];
	/**
	 * Role-based authorization: which roles may reach each action/surface/datasource (ADR 0028).
	 */
	rules?: {
		actions?: RoleMap;
		surfaces?: RoleMap;
		data?: {
			[k: string]: {
				[k: string]: string[];
			};
		};
	};
}
interface SecurityProvider {
	id: string;
	/**
	 * oidc: social/generic OIDC (Authorization Code + PKCE). password: local username/password.
	 */
	type: "oidc" | "password";
	/**
	 * OIDC preset shorthand (e.g. google, github, auth0).
	 */
	preset?: string;
	clientId?: string;
	clientSecret?: string;
	/**
	 * password providers: self sign-up policy.
	 */
	signup?: "open" | "invite" | "closed";
}
interface RoleMap {
	[k: string]: string[];
}

export {};
