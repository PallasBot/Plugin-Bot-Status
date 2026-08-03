# ruff: noqa: E402

import nonebot

nonebot.init()

from pallas.api.platform import FederatePeerBotRoster

from pallas_plugin_bot_status.list_mode import format_federate_status_rosters


def test_federate_status_lists_public_accounts_one_per_line():
    text = format_federate_status_rosters([
        FederatePeerBotRoster(
            deployment_id="dep-a",
            deployment_name="部署 A",
            bot_ids=frozenset({10001, 10002, 10003}),
            online_bot_ids=frozenset({10001, 10002, 10003}),
            public_bot_ids=frozenset({10001, 10002}),
        ),
        FederatePeerBotRoster(
            deployment_id="dep-b",
            deployment_name="部署 B",
            bot_ids=frozenset({20001, 20002}),
            online_bot_ids=frozenset({20001, 20002}),
            public_bot_ids=frozenset(),
        ),
        FederatePeerBotRoster(
            deployment_id="dep-legacy",
            deployment_name="",
            bot_ids=frozenset({30001}),
            online_bot_ids=None,
            public_bot_ids=frozenset(),
        ),
    ], nicknames_by_id={10001: "牛牛一"})

    assert text == (
        "部署 A：3只在线\n"
        "牛牛一 (10001)\n"
        "QQ 10002\n"
        "另有 1 只在线未公开 QQ\n\n"
        "部署 B：2只在线\n"
        "2 只在线未公开 QQ\n\n"
        "部署 dep-leg：状态未知（对端待升级）"
    )
