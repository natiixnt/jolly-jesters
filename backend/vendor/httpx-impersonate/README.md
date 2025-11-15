# httpx-impersonate (local fallback)

This lightweight package provides a local fallback implementation of the
`httpx-impersonate` interface for environments where the upstream project
cannot be installed directly.  It exposes a `Client` class that proxies
requests through [`tls-client`](https://pypi.org/project/tls-client/) to
obtain hardened TLS fingerprints that mimic modern Chrome releases while
remaining API-compatible with the original library for the parts used by
our backend services.
