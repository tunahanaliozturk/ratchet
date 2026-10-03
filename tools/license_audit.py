"""Fails when any installed distribution is not under a permissive licence, at any depth of the tree.

It reads the metadata of what is installed rather than asking a web service, so it gives the same answer offline and
in CI. The order of evidence is PEP 639 License-Expression, then the License field, then the trove classifiers, and
last, for a distribution with no licence metadata at all, the text of the licence file it bundles.
A distribution whose licence still cannot be determined fails: somebody has to look at it and record why it is fine.
"""

import re
import sys
from importlib.metadata import Distribution, distributions
from pathlib import Path

PROJECT = "ratchet"

ALLOWED = {
    "MIT",
    "MIT-0",
    "MIT-CMU",  # the Pillow licence: OSI-approved, permissive, attribution only
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
    "Zlib",
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

# Text that identifies a licence file beyond doubt, for packages that ship the file but no metadata.
LICENCE_TEXT = {
    "Permission is hereby granted, free of charge, to any person obtaining a copy": "MIT",
    "Version 2.0, January 2004": "Apache-2.0",
}

# Reviewed by hand. Each entry says why, so the exception can be challenged later.
REVIEWED: dict[str, str] = {}

_CLASSIFIER = re.compile(r"^License :: OSI Approved :: (.+)$")
_SHORT = 80  # a License field longer than this is the whole licence text, not a name


def _spdx_terms(expression: str) -> set[str]:
    return {t.strip() for t in re.split(r"\s+(?:AND|OR|WITH)\s+|[()]", expression) if t.strip()}


def _allowed_expression(expression: str) -> bool:
    """For OR, one allowed term is enough; otherwise every term must be allowed."""
    terms = _spdx_terms(expression)
    return bool(terms & ALLOWED) if " OR " in expression else terms <= ALLOWED


def _bundled(dist: Distribution) -> str | None:
    for file in dist.files or []:
        if "licen" not in file.name.lower():
            continue
        path = Path(str(file.locate()))
        text = path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""
        for marker, spdx in LICENCE_TEXT.items():
            if marker in text:
                return spdx
    return None


def verdict(dist: Distribution) -> tuple[bool, str]:
    meta = dist.metadata
    name = meta["Name"]
    expression = meta.get("License-Expression")
    field = (meta.get("License") or "").strip()
    if field and ("\n" in field or len(field) >= _SHORT):
        field = ""
    classifiers = [m.group(1) for c in meta.get_all("Classifier") or [] if (m := _CLASSIFIER.match(c))]

    if name.lower() in REVIEWED:
        found: tuple[bool, str] = (True, f"reviewed: {REVIEWED[name.lower()]}")
    elif expression:
        found = (_allowed_expression(expression), expression)
    elif field and _allowed_expression(ALIASES.get(field.lower(), field)):
        found = (True, field)
    elif classifiers and {ALIASES.get(c.lower(), c) for c in classifiers} <= ALLOWED:
        found = (True, ", ".join(sorted(classifiers)))
    elif not (field or classifiers) and (bundled := _bundled(dist)):
        found = (True, f"{bundled}, recognised from the bundled licence file")
    else:
        found = (False, field[:60] or ", ".join(classifiers) or "no licence metadata")
    return found


def main() -> int:
    failures = []
    seen = set()
    for dist in sorted(distributions(), key=lambda d: d.metadata["Name"].lower()):
        name = dist.metadata["Name"]
        if name.lower() in seen or name.lower() == PROJECT:
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
