/** The scalar types a `nano:extend` field may declare (mirrors the vocabulary
 *  the console's shape scan understands). `integer`/`number` are both `number`
 *  in TS; `datetime` is an ISO `string`. */
export type ScalarType = "string" | "integer" | "number" | "boolean" | "datetime";
/** Map a scalar envelope type to its TypeScript representation. */
export type ScalarTs<T extends ScalarType> = T extends "string" | "datetime" ? string : T extends "integer" | "number" ? number : T extends "boolean" ? boolean : never;
/** A field spec: either a bare scalar type, or an object with modifiers. */
export type FieldSpec = ScalarType | {
    type: ScalarType;
    optional?: boolean;
    list?: boolean;
};
/** The TS type of a single field spec (applies `list` before `optional`). */
export type FieldTs<F> = F extends ScalarType ? ScalarTs<F> : F extends {
    type: infer T extends ScalarType;
    list: true;
} ? ScalarTs<T>[] : F extends {
    type: infer T extends ScalarType;
} ? ScalarTs<T> : never;
type OptionalKeys<S> = {
    [K in keyof S]: S[K] extends {
        optional: true;
    } ? K : never;
}[keyof S];
type RequiredKeys<S> = Exclude<keyof S, OptionalKeys<S>>;
/** The inferred payload type of an envelope: required + optional fields. */
export type EnvelopeType<S extends Record<string, FieldSpec>> = {
    [K in RequiredKeys<S>]: FieldTs<S[K]>;
} & {
    [K in OptionalKeys<S>]?: FieldTs<S[K]>;
};
/** A normalised field, as stored on an `Envelope` and lifted to `nano:extend`. */
export interface EnvelopeField {
    name: string;
    type: ScalarType;
    optional: boolean;
    list: boolean;
}
/**
 * A typed data envelope: a named schema (for the model) plus a phantom TS type
 * (for the call site). Construct with {@link envelope}. `type` is a phantom
 * property — it is `undefined` at runtime and exists only to carry the inferred
 * payload type (use `typeof env.type` in type positions).
 */
export interface Envelope<S extends Record<string, FieldSpec> = Record<string, FieldSpec>> {
    readonly name: string;
    readonly fields: EnvelopeField[];
    /** Phantom: the inferred payload type. Do not read at runtime. */
    readonly type: EnvelopeType<S>;
}
/**
 * Declare a typed data envelope. `name` becomes the `nano:shape` id lifted into
 * the model (must be a valid BPMN identifier); `fields` declares the payload.
 * The returned envelope's `type` phantom carries the inferred TS payload type.
 */
export declare function envelope<const S extends Record<string, FieldSpec>>(name: string, fields: S): Envelope<S>;
export {};
