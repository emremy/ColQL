export class ColQLError extends Error {
  readonly code: string;
  readonly details?: unknown;

  constructor(code: string, message: string, details?: unknown) {
    super(message);
    this.name = "ColQLError";
    this.code = code;
    this.details = details;
  }
}

export function colqlError(code: string, message: string, details?: unknown): ColQLError {
  return new ColQLError(code, message, details);
}
