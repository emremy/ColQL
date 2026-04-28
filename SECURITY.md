# Security Policy

## Supported Versions

ColQL is currently in early development (`v0.0.x`).

We recommend always using the latest version, as security fixes and improvements will only be applied to the most recent release.

| Version | Supported |
|--------|----------|
| 0.0.x  | ✅ Yes    |
| < 0.0.x | ❌ No    |

---

## Reporting a Vulnerability

If you discover a security vulnerability in ColQL, please report it responsibly.

### How to Report

- Open a **private security advisory** on GitHub (preferred), or
- Contact us via email (if available), or
- Open an issue **only if the vulnerability is not sensitive**

### Please Include

- A clear description of the vulnerability
- Steps to reproduce
- Impact assessment (if known)
- Any potential fixes or suggestions (optional)

---

## Response Policy

We aim to:

- Acknowledge reports within **48 hours**
- Provide an initial assessment within **3–5 days**
- Release a fix as soon as possible, depending on severity

---

## Scope

ColQL is an in-memory query engine. Security considerations mainly include:

- Memory safety and unexpected data exposure
- Input validation and runtime errors
- Denial-of-service scenarios via malformed queries or large inputs
- Serialization/deserialization integrity

---

## Out of Scope

The following are generally out of scope unless they lead to a real vulnerability:

- Performance issues (unless exploitable)
- Incorrect usage of the library
- Theoretical or non-reproducible issues

---

## Best Practices for Users

To use ColQL securely:

- Do not treat `rowIndex` as a stable identifier
- Validate user input before passing it into queries
- Avoid exposing raw query interfaces directly to untrusted users
- Use explicit `id` fields for identity management

---

## Disclosure

We follow responsible disclosure:

- Vulnerabilities will not be publicly disclosed until a fix is available
- Users will be notified via release notes when fixes are shipped
