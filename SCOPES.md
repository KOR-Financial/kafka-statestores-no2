# Commit Scopes

Valid scopes for commits in this repository. A scope names the section of the code that changed. It is never the repository name: `kafka-statestores-no2` and `statestores` say nothing, because every commit here touches kafka-statestores-no2.

This repository looks archived: the last commit is from August 2022. It also holds patched code from the Nitrite library under `org/dizitart/no2/`.

## Library sections

| Scope | When to use |
|---|---|
| `statestore` | The Kafka Streams document state store and its serialization: `src/main/java/io/techasylum/kafka/statestore/document/` |
| `nitrite` | The patched Nitrite filters, mixins and mapper: `src/main/java/org/dizitart/no2/` |

## Infrastructure

| Scope | When to use |
|---|---|
| `build` | `pom.xml` (except dependency versions) |
| `deps` | Dependency version bumps |
| `ci` | GitHub Actions workflows: `.github/workflows/` |
| `release` | Maven release plugin commits (`[maven-release-plugin] prepare release ...`) |
| `docs` | `README.md` |

## Multiple contexts

Comma-separate when one commit intentionally touches two distinct sections:

```
fix(statestore,nitrite): Fixed the filter deserialization of nested documents
```

Omit the scope only when the change is so sweeping that listing every scope would be noise, or when no section applies (for example a root `.gitignore`). For anything that touches code, name the section.
