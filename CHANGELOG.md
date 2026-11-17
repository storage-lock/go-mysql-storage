# Changelog

## v0.0.4
_发布日期：2026-10-24_

- build: require published go-events v0.0.5

## v0.0.3
_发布日期：2026-10-12_

- build: 依赖改为已发布版本，移除本地 replace
- 更新 go-mysql-storage
- fix(connection-manager): 漏洞4 — Take 失败不缓存 err，可重试
- feat: 实现 Capabilities() 方法
- fix: MySQL CreateWithVersion 错误码映射修正
- Merge pull request #8 from storage-lock/dev
- refactor: rename function
- Merge pull request #7 from storage-lock/dev
- fix: mysql内置的主键冲突转换为StorageLock内置的版本miss错误
- Merge pull request #6 from storage-lock/dev
- fix: mysql内置的主键冲突转换为StorageLock内置的版本miss错误
- Merge pull request #5 from storage-lock/dev
- ConnectionManager支持指定db
- Merge pull request #4 from storage-lock/dev
- refactor
- Merge pull request #3 from storage-lock/dev
- refactor: 修改为基于SqlStorage实现

## v0.0.2
_发布日期：2023-08-07_

- 文档更新
- 文档及Example完善
- upgrade storage from v0.0.1 to v0.0.2
- go sdk set 1.19
- ci: go sdk change to 1.18
- func rename
- typo fix
- 降低go sdk版本
- upgrade dependencies
- refactor & review
- docs: 完善文档
- ci: add ci

## v0.0.1
_发布日期：2023-08-06_

- fix: update helper

## Unreleased

- build: 升级依赖到 go-storage/go-storage-lock v0.1.0
