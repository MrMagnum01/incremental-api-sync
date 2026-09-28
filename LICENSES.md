# Licenses

## Runtime

No third-party runtime dependencies. Everything used (`sqlite3`, `fcntl`, `hashlib`, `json`, `uuid`, `email.utils`, `argparse`, `dataclasses`, `subprocess`, `unittest`, ...) is part of the Python 3 standard library, distributed under the [Python Software Foundation License](https://docs.python.org/3/license.html) (PSF License, OSI-approved).

## Development / test

No additional test framework — the test suite uses `unittest`, also standard library.

## Tooling used to prepare this repository (not shipped)

- `gitleaks` (MIT License, OSI-approved) — used only to scan the working tree and history before delivery; not a project dependency.
