# Security

## Reporting

Please report vulnerabilities privately through GitHub's "Report a vulnerability" on this repository, not in a public
issue. You should hear back within a week.

## What the code does about it

- **Every SQL statement is parameterised.** The few f-strings in `store.py` interpolate module constants (column lists,
  a shared CTE), never values, and each carries a comment saying so.
- **The API refuses anonymous use.** It will not start without `RATCHET_API_TOKEN`, and compares the presented token
  with `hmac.compare_digest`.
- **Request bodies are capped** (`RATCHET_MAX_PAYLOAD_BYTES`), counted as bytes arrive, so leaving out
  `Content-Length` does not get round it.
- **Inputs are validated before they are stored.** A workflow's input is checked against its annotation at start time,
  and ids and signal names are restricted to a safe character set.
- **Errors are stored, secrets are not.** A failed workflow records the exception type, message and traceback. Do not
  put credentials in exception messages raised from activities; they would end up in the database and in API responses.
- **The image runs as a non-root user** without pip, and CI scans it and the locked dependencies for known
  vulnerabilities.

## What it does not do

- The shared token is all-or-nothing. Anyone holding it can start, signal and cancel any workflow. Put the API behind
  your own gateway for per-caller identity and authorisation.
- Workflow inputs, results and signal payloads are stored in plain text. Encrypt sensitive fields before they reach
  Ratchet, or keep them elsewhere and pass references.
