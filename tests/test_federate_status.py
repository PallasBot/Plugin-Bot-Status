# ruff: noqa: E402

import nonebot

nonebot.init()

from pallas.api.platform import FederatePeerBotRoster

from pallas_plugin_bot_status.config import Config
from pallas_plugin_bot_status.list_mode import (
    format_federate_status_rosters,
    should_show_federate_status,
)


def test_other_deployments_are_shown_by_default() -> None:
    config = Config()

    assert config.bot_status_show_other_deployments is True
    assert should_show_federate_status(federate_active=True, config=config) is True


def test_other_deployments_can_be_hidden() -> None:
    config = Config(bot_status_show_other_deployments=False)

    assert should_show_federate_status(federate_active=True, config=config) is False


def test_federate_status_is_hidden_when_federation_is_inactive() -> None:
    assert should_show_federate_status(federate_active=False, config=Config()) is False


def test_local_status_command_fans_out_to_each_deployment() -> None:
    from pallas_plugin_bot_status import __plugin_meta__

    extra = __plugin_meta__.extra
    assert "我的牛牛" in extra["exact_plaintexts"]
    assert extra["ingress_fanout"] == {
        "scope": "shard_only",
        "plaintexts": ["牛牛报数", "牛牛出列"],
        "normalize_trailing_punct": True,
    }
    assert extra["ingress_fanout_additional"] == [
        {
            "scope": "always",
            "plaintexts": ["我的牛牛"],
            "normalize_trailing_punct": True,
        },
    ]


def test_federate_status_summarizes_unavailable_peer_without_upgrade_prompt():
    text = format_federate_status_rosters([
        FederatePeerBotRoster(
            deployment_id="dep-legacy",
            deployment_name="旧部署",
            bot_ids=frozenset({30001}),
            online_bot_ids=None,
            public_bot_ids=frozenset(),
        ),
    ])

    assert text == "旧部署：在线状态暂不可用"


def test_federate_status_lists_public_accounts_one_per_line():
    text = format_federate_status_rosters([
        FederatePeerBotRoster(
            deployment_id="dep-a",
            deployment_name="部署 A",
            bot_ids=frozenset({10001, 10002, 10003}),
            online_bot_ids=frozenset({10001, 10002, 10003}),
            public_bot_ids=frozenset({10001, 10002}),
            public_online_bot_names={10001: "快照牛牛"},
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
    ])

    assert text == (
        "部署 A：3只在线\n"
        "快照牛牛 (10001)\n"
        "QQ 10002\n"
        "另有 1 只在线未公开 QQ\n\n"
        "部署 B：2只在线\n"
        "2 只在线未公开 QQ\n\n"
        "部署 dep-leg：在线状态暂不可用"
    )
