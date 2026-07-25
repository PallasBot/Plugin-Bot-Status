# 更新日志

本文件依据 git tag 历史整理，版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。
新提交合入后请在 `## [Unreleased]` 下记录，发布时随版本 tag 归档。

## [Unreleased]


## [$new] - $TODAY

- chore(ci): 对齐 ruff 0.16 与主仓 ruff.toml，修复 lint

## [4.0.13] - 2026-07-06
- fix(status): 牛牛在吗在协议端已报离线时优先展示离线，避免僵尸 WS 仍显示在线

## [4.0.12] - 2026-06-28
- refactor(config): 名册模式 WebUI 说明去 fleet/worker/WS 等维护者黑话

## [4.0.11] - 2026-06-27
- fix(offline_mail): 批量发信按号主合并、间隔与 Connection lost 重试，回复展示号主 QQ

## [4.0.10] - 2026-06-27
- feat(offline_mail): 新增「离线邮件」超管命令，仅向离线牛牛号主发信

## [4.0.9] - 2026-06-27
- docs(readme): 命令权限默认等级改用中文展示

## [4.0.8] - 2026-06-26
- chore(assets): 更新品牌图为「牛牛报到」透明背景版本

## [4.0.7] - 2026-06-25
- feat(metadata): 补充测试邮件命令冷却声明

## [4.0.6] - 2026-06-24
- feat(knowledge): 声明 knowledge_sources FAQ 供 LLM 注入

## [4.0.5] - 2026-06-19
- docs: 改用 PyPI 版本徽章
- chore(assets): 替换品牌头像为透明背景版本

## [4.0.4] - 2026-06-18
- refactor(bot_status): 精简邮件与状态逻辑并更新插件说明

## [4.0.3] - 2026-06-18
- migrate: src.* → pallas.api.* / pallas.product.* / pallas.core.*
- release: bump to 4.0.3 for pallas import migration

## [4.0.2] - 2026-06-18
- docs(readme): 添加 Pallas-Bot hero 图
- chore(release): 4.0.2 同步 README 进 PyPI 包

## [4.0.1] - 2026-06-17
- feat: 初始发布 pallas-plugin-bot-status 4.0.1
