import { unflatten } from "flat";
import isArray from "lodash/isArray";
import isObject from "lodash/isObject";
import type { TLocalizedValidationError } from "typebox/error";
import Value from "typebox/value";
import { AppConfig, type AppConfigRaw } from "./config.schema";

/**
 * Builds AppConfig from nested environment variables using `__` as the path separator
 * ("ANYTYPE__API_URL", "LLM__0__NAME"): POSIX env names cannot contain dots, so a dotted
 * key never reaches a container. Applies schema defaults and TypeBox corrective coercion
 * (e.g. "800" -> 800).
 *
 * Fails fast with a readable field list instead of TypeBox's opaque `ParseError`
 * (whose message is just "Parse" and hides the failing paths in `cause.errors`).
 */
export const loadFromEnv = (env: NodeJS.ProcessEnv = process.env): AppConfig => {
  const raw = unflatten<NodeJS.ProcessEnv, AppConfigRaw>(env, { delimiter: "__" });

  // A lone provider may be written without an index ("LLM__MODE"), which unflattens to an
  // object rather than a one-element array.
  if (isObject(raw.LLM) && !isArray(raw.LLM)) raw.LLM = [raw.LLM];

  try {
    // `Value.Default` MUST stay explicit and run before `Parse`: `Parse` first runs
    // `Check` and returns the value as-is when it already validates, skipping its
    // internal Default step. Without this, a defaulted-but-optional field (or any field
    // whose absence still satisfies the schema) silently stays undefined.
    return Value.Parse(AppConfig, Value.Default(AppConfig, raw));
  } catch (err: unknown) {
    const errors = validationErrorsOf(err);
    if (!errors) throw err;

    const detail = errors.map((e) => `  - ${e.instancePath || "/"}: ${e.message}`).join("\n");
    throw new Error(`[Config] Invalid environment variables:\n${detail}`, { cause: err });
  }
};

function validationErrorsOf(err: unknown): TLocalizedValidationError[] | null {
  const errors = (err as { cause?: { errors?: unknown } }).cause?.errors;
  return isArray(errors) ? (errors as TLocalizedValidationError[]) : null;
}
