"""Fails when any installed distribution is not under a permissive licence, at any depth of the tree.

It reads the metadata of what is installed rather than asking a web service, so it gives the same answer offline and
in CI. The order of evidence is PEP 639 License-Expression, then the License field, then the trove classifiers.
A distribution whose licence cannot be determined fails too: somebody has to look at it and record why it is fine.
"""

import re
import sys
from importlib.metadata import Distribution, distributions

ALLOWED = {
    "MIT",
    "MIT-0",
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "MS-PL",
    "PostgreSQL",
    "0BSD",
    "Unlicense",
    "MPL-2.0",
    "PSF-2.0",
    "Python-2.0",
    "CC0-1.0",
}

# Free-text License fields and classifiers, mapped to the SPDX identifier they mean.
ALIASES = {
    "mit": "MIT",
    "mit license": "MIT",
    "the mit license": "MIT",
    "apache 2.0": "Apache-2.0",
    "apache-2.0": "Apache-2.0",
    "apache license 2.0": "Apache-2.0",
    "apache software license": "Apache-2.0",
    "apache license, version 2.0": "Apache-2.0",
    "bsd": "BSD-3-Clause",
    "bsd license": "BSD-3-Clause",
    "new bsd license": "BSD-3-Clause",
    "3-clause bsd license": "BSD-3-Clause",
    "bsd-3-clause": "BSD-3-Clause",
    "bsd 3-clause": "BSD-3-Clause",
    "bsd 2-clause license": "BSD-2-Clause",
    "bsd-2-clause": "BSD-2-Clause",
    "isc license": "ISC",
    "isc license (iscl)": "ISC",
    "mozilla public license 2.0 (mpl 2.0)": "MPL-2.0",
    "the unlicense (unlicense)": "Unlicense",
    "unlicense": "Unlicense",
    "python software foundation license": "PSF-2.0",
    "mit or apache-2.0": "MIT",
}

# Reviewed by hand. Each entry says why, so the exception can be challenged later.
REVIEWED: dict[str, str] = {}

_CLASSIFIER = re.compile(r"^License :: OSI Approved :: (.+)$")


def _spdx_terms(expression: str) -> set[str]:
    """Every licence an SPDX expression names. For OR, one allowed term is enough, which the caller decides."""
    return {t for t in re.split(r"\s+(?:AND|OR|WITH)\s+|[()]", expression) if t.strip()}


def verdict(dist: Distribution) -> tuple[bool, str]:
    meta = dist.metadata
    name = meta["Name"]
    if name.lower() in REVIEWED:
        return True, f"reviewed: {REVIEWED[name.lower()]}"
    expression = meta.get("License-Expression")
    if expression:
        terms = _spdx_terms(expression)
        if " OR " in expression:
            return bool(terms & ALLOWED), expression
        return terms <= ALLOWED, expression
    field = (meta.get("License") or "").strip()
    if field and "\n" not in field and len(field) < 80:  # longer means the whole licence text
        spdx = ALIASES.get(field.lower(), field)
        if spdx in ALLOWED:
            return True, spdx
    classifiers = [m.group(1) for c in meta.get_all("Classifier") or [] if (m := _CLASSIFIER.match(c))]
    mapped = {ALIASES.get(c.lower(), c) for c in classifiers}
    if mapped and mapped <= ALLOWED:
        return True, ", ".join(sorted(mapped))
    return False, expression or field[:60] or ", ".join(classifiers) or "no licence metadata"


def main() -> int:
    failures = []
    seen = set()
    for dist in sorted(distributions(), key=lambda d: d.metadata["Name"].lower()):
        name = dist.metadata["Name"]
        if name.lower() in seen or name.lower() == "ratchet":
            continue
        seen.add(name.lower())
        ok, evidence = verdict(dist)
        print(f"{'ok  ' if ok else 'FAIL'} {name} {dist.version}: {evidence}")
        if not ok:
            failures.append(name)
    if failures:
        print(f"\n{len(failures)} distribution(s) need a decision: {', '.join(failures)}", file=sys.stderr)
        return 1
    print(f"\n{len(seen)} distributions, all permissive")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
