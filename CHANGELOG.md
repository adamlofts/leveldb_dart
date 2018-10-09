# Changelog

## 6.0.1

Non-breaking changes:

- Add OSX platform support

## 6.0.0

Breaking changes:
- Upgrade to dart2

## 5.0.1

Non-breaking changes

- Add @required annotation to keyEncoding and valueEncoding in LevelDB.open
- Improve docs

## 5.0.0

Breaking changes:

- Remove `LevelEncoding` interface and use `dart:codec` directly. This better aligns the interface with
the dart way of encoding and decoding and allows easily fusing new codecs. 
- Add new json.dart example to demonstrate encoding objects to the database (as JSON).

## 4.0.0

Minor API update for [Sound Dart](https://www.dartlang.org/guides/language/sound-dart)
 
Breaking changes:
- The `keyEncoding` and `valueEncoding` parameters are now required when using the `LevelDB.open` function.
When encoding utf8 keys and values `LevelDB.openUtf8` is the recommended constructor. 

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
