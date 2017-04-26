# Changelog

## 3.0.0

Breaking changes:

- Add generic parameters to `LevelDB` to improve type safety when using the API.
Key/Value encoding parameters have been moved to the `LevelDB.open` function.
- Minimum dart sdk version updated to `1.23.0`

Non-breaking changes:

- Upgrade to leveldb 1.20. This version is compatible with the previous on-disk format. See: https://github.com/google/leveldb/releases/tag/v1.20
- Add `shared` parameter to `LevelDB.open`. This feature allows referencing
the same underlying database from multiple isolates.
- Add an example demonstrating how to use the `shared` parameter in muliple
isolates.

## 2.0.3

- Build leveldb with better compatibility.
