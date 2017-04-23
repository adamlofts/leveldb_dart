# Changelog

## 3.0.0

- Upgrade to leveldb 1.20. https://github.com/google/leveldb/releases/tag/v1.20
- Add `shared` parameter to `LevelDB.open`. This feature allows referencing
the same underlying database from multiple isolates.

## 2.0.3

- Build leveldb with better compatibility.
