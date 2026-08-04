from __future__ import annotations

import nonebot
import pytest

nonebot.init()


@pytest.mark.asyncio
async def test_unified_handler_sends_for_all_local_bots(monkeypatch: pytest.MonkeyPatch) -> None:
    from pallas_plugin_bot_status import shard_count

    sent: list[tuple[int, int, str]] = []
    registrations: list[list[int]] = []
    finished: list[str] = []
    completion_claims: list[int] = []
    resolved_groups: list[int] = []
    sleep_calls: list[float] = []
    clock = [0.0]

    class Bot:
        self_id = "100"

    class Event:
        group_id = 10086
        user_id = 20001
        time = 30002

        def get_plaintext(self) -> str:
            return "牛牛报数"

    async def list_local_bots(_group_id: int) -> list[int]:
        return [100]

    async def resolve_local_bots(group_id: int) -> list[int]:
        resolved_groups.append(group_id)
        return [100, 300]

    async def run_coord(**_kwargs) -> tuple[int, int]:
        return 2, 3

    async def update_registration(**kwargs) -> None:
        registrations.append(kwargs["bot_ids"])

    async def read_order(**_kwargs) -> list[int]:
        return [200, 100, 300]

    async def send_as_bot(bot_id: int, group_id: int, message: str) -> bool:
        sent.append((bot_id, group_id, message))
        clock[0] += 0.2
        return True

    async def claim_completion(**kwargs) -> bool:
        completion_claims.append(kwargs["bot_id"])
        return len(completion_claims) == 3

    async def finish(message: str) -> None:
        finished.append(message)

    async def sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(shard_count.shard_ctx, "sharding_active", lambda: False)
    monkeypatch.setattr(shard_count, "list_local_fleet_bots_in_group", list_local_bots)
    monkeypatch.setattr(shard_count, "resolve_local_connected_bots_in_group", resolve_local_bots)
    monkeypatch.setattr(shard_count, "run_shard_coordinated_bot_count", run_coord)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", update_registration)
    monkeypatch.setattr(shard_count, "get_shard_bot_count_order", read_order)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)
    monkeypatch.setattr(shard_count, "mark_shard_bot_count_reported_and_claim_completion", claim_completion)
    monkeypatch.setattr(shard_count.asyncio, "sleep", sleep)
    monkeypatch.setattr(shard_count.time, "monotonic", lambda: clock[0])

    await shard_count.handle_shard_bot_count(Bot(), Event(), finish=finish)

    assert registrations == [[100, 300]]
    assert resolved_groups == [10086]
    assert sent == [
        (100, 10086, "牛牛2号报到！"),
        (300, 10086, "牛牛3号报到！"),
    ]
    assert completion_claims == [100, 300, 300]
    assert sleep_calls == pytest.approx([0.35, 0.15, 0.8, 0.3])
    assert finished == ["牛牛们报数完毕！"]


@pytest.mark.asyncio
async def test_unified_non_representative_does_not_proxy_the_full_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from pallas_plugin_bot_status import shard_count

    sent: list[int] = []

    class Bot:
        self_id = "300"

    class Event:
        group_id = 10086
        user_id = 20001
        time = 30002

        def get_plaintext(self) -> str:
            return "牛牛报数"

    async def list_local_bots(_group_id: int) -> list[int]:
        return [100, 300]

    async def resolve_local_bots(_group_id: int) -> list[int]:
        return [100, 300]

    async def run_coord(**_kwargs) -> tuple[int, int]:
        return 2, 2

    async def update_registration(**_kwargs) -> None:
        return None

    async def read_order(**_kwargs) -> list[int]:
        return [100, 300]

    async def send_as_bot(bot_id: int, *_args) -> bool:
        sent.append(bot_id)
        return True

    monkeypatch.setattr(shard_count.shard_ctx, "sharding_active", lambda: False)
    monkeypatch.setattr(shard_count, "list_local_fleet_bots_in_group", list_local_bots)
    monkeypatch.setattr(shard_count, "resolve_local_connected_bots_in_group", resolve_local_bots)
    monkeypatch.setattr(shard_count, "run_shard_coordinated_bot_count", run_coord)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", update_registration)
    monkeypatch.setattr(shard_count, "get_shard_bot_count_order", read_order)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)

    await shard_count.handle_shard_bot_count(Bot(), Event(), finish=lambda _message: None)

    assert sent == []


@pytest.mark.asyncio
async def test_unified_handler_does_not_count_unsent_bot_as_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    from pallas_plugin_bot_status import shard_count

    completion_claims: list[int] = []

    class Bot:
        self_id = "100"

    class Event:
        group_id = 10086
        user_id = 20001
        time = 30002

        def get_plaintext(self) -> str:
            return "牛牛报数"

    async def list_local_bots(_group_id: int) -> list[int]:
        return [100]

    async def resolve_local_bots(_group_id: int) -> list[int]:
        return [100]

    async def run_coord(**_kwargs) -> tuple[int, int]:
        return 1, 1

    async def update_registration(**_kwargs) -> None:
        return None

    async def read_order(**_kwargs) -> list[int]:
        return [100]

    async def send_as_bot(*_args) -> bool:
        return False

    async def claim_completion(**kwargs) -> bool:
        completion_claims.append(kwargs["bot_id"])
        return True

    monkeypatch.setattr(shard_count.shard_ctx, "sharding_active", lambda: False)
    monkeypatch.setattr(shard_count, "list_local_fleet_bots_in_group", list_local_bots)
    monkeypatch.setattr(shard_count, "resolve_local_connected_bots_in_group", resolve_local_bots)
    monkeypatch.setattr(shard_count, "run_shard_coordinated_bot_count", run_coord)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", update_registration)
    monkeypatch.setattr(shard_count, "get_shard_bot_count_order", read_order)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)

    async def sleep(_seconds: float) -> None:
        return None

    async def finish(_message: str) -> None:
        return None

    monkeypatch.setattr(shard_count, "mark_shard_bot_count_reported_and_claim_completion", claim_completion)
    monkeypatch.setattr(shard_count.asyncio, "sleep", sleep)

    await shard_count.handle_shard_bot_count(Bot(), Event(), finish=finish)

    assert completion_claims == []
