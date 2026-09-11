# Security policy

## Reporting a vulnerability

Do not open a public issue. Email the maintainer through the address on the
[Rubrum95](https://github.com/Rubrum95) GitHub profile with:

- the affected component (crate, route, binary) and version or commit,
- steps to reproduce or a proof of concept,
- the impact you assess.

You will get an acknowledgement within 72 hours and a fix or mitigation plan within 14 days for
confirmed issues. Please allow the fix to ship before disclosing.

## Scope

The services in this repository read public chain data and serve it over HTTP. They hold no user
accounts, keys or funds. Findings of interest: request handling (denial of service through
expensive routes, rate-limit bypass), SQL or decoding errors that corrupt indexed data, and
dependency vulnerabilities.
