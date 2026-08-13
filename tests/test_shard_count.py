from __future__ import annotations

import asyncio
from types import SimpleNamespace

import nonebot
import pytest

nonebot.init()


@pytest.mark.asyncio
async def test_unified_runner_sends_completion_as_last_reporting_bot(monkeypatch: pytest.MonkeyPatch) -> None:
    from pallas_plugin_bot_status import shard_count

    sent: list[tuple[int, int, str]] = []
    registrations: list[list[int]] = []
    completion_claims: list[tuple[int, bool]] = []
    resolved_groups: list[int] = []
    sleep_calls: list[float] = []
    clock = [0.0]
    notices: list[tuple[int, str]] = []

    class Bot:
        self_id = "100"

        async def send_group_msg(self, *, group_id: int, message: str) -> None:
            notices.append((group_id, message))

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

    async def wait_turn(**_kwargs) -> bool:
        return True

    async def send_as_bot(bot_id: int, group_id: int, message: str) -> bool:
        sent.append((bot_id, group_id, message))
        clock[0] += 0.2
        return True

    async def claim_completion(**kwargs) -> bool:
        completion_claims.append((kwargs["bot_id"], kwargs.get("allow_timeout", True)))
        return len(completion_claims) == 3

    async def sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(shard_count.shard_ctx, "sharding_active", lambda: False)
    monkeypatch.setattr(shard_count, "list_local_fleet_bots_in_group", list_local_bots)
    monkeypatch.setattr(shard_count, "resolve_local_connected_bots_in_group", resolve_local_bots)
    monkeypatch.setattr(shard_count, "run_shard_coordinated_bot_count", run_coord)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", update_registration)
    monkeypatch.setattr(shard_count, "get_shard_bot_count_order", read_order)
    monkeypatch.setattr(shard_count, "wait_shard_bot_count_turn", wait_turn)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)
    monkeypatch.setattr(shard_count, "mark_shard_bot_count_reported_and_claim_completion", claim_completion)
    monkeypatch.setattr(shard_count.asyncio, "sleep", sleep)
    monkeypatch.setattr(shard_count.time, "monotonic", lambda: clock[0])

    await shard_count.run_shard_bot_count(Bot(), Event())

    assert registrations == [[100, 300]]
    assert resolved_groups == [10086]
    assert notices == []
    assert sent == [
        (100, 10086, "牛牛2号报到！"),
        (300, 10086, "牛牛3号报到！"),
        (300, 10086, "牛牛们报数完毕！"),
    ]
    assert completion_claims == [(100, False), (300, False), (300, True)]
    assert sleep_calls == pytest.approx([0.35, 0.15, 0.8, 0.3])


@pytest.mark.asyncio
async def test_shard_handler_starts_one_background_task_per_group(monkeypatch: pytest.MonkeyPatch) -> None:
    from pallas_plugin_bot_status import shard_count

    started = asyncio.Event()
    release = asyncio.Event()

    class Bot:
        self_id = "100"

    class Event:
        group_id = 10086
        user_id = 20001
        time = 30002

        def get_plaintext(self) -> str:
            return "牛牛报数"

    async def run_count(_bot: Bot, _event: Event) -> None:
        started.set()
        await release.wait()

    monkeypatch.setattr(shard_count, "run_shard_bot_count", run_count)

    assert await shard_count.handle_shard_bot_count(Bot(), Event())
    await asyncio.wait_for(started.wait(), timeout=0.1)
    assert not await shard_count.handle_shard_bot_count(Bot(), Event())

    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert await shard_count.handle_shard_bot_count(Bot(), Event())
    for task in tuple(shard_count.bot_count_tasks.values()):
        task.cancel()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_shard_handler_registers_late_local_bot_for_active_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pallas_plugin_bot_status import shard_count

    started = asyncio.Event()
    release = asyncio.Event()
    registrations: list[dict[str, object]] = []

    class Event:
        group_id = 10086
        user_id = 20001
        time = 30002

        def get_plaintext(self) -> str:
            return "牛牛报数"

    async def run_count(_bot: object, _event: Event) -> None:
        started.set()
        await release.wait()

    async def register(**kwargs: object) -> None:
        registrations.append(kwargs)

    monkeypatch.setattr(shard_count, "run_shard_bot_count", run_count)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", register)

    assert await shard_count.handle_shard_bot_count(SimpleNamespace(self_id="100"), Event())
    await asyncio.wait_for(started.wait(), timeout=0.1)
    assert not await shard_count.handle_shard_bot_count(SimpleNamespace(self_id="200"), Event())
    assert registrations == [
        {
            "group_id": 10086,
            "user_id": 20001,
            "plaintext": "牛牛报数",
            "message_time": 30002,
            "bot_ids": [200],
        }
    ]

    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_unified_claim_winner_proxies_the_full_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from pallas_plugin_bot_status import shard_count

    sent: list[int] = []
    notices: list[tuple[int, str]] = []

    class Bot:
        self_id = "300"

        async def send_group_msg(self, *, group_id: int, message: str) -> None:
            notices.append((group_id, message))

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

    async def wait_turn(**_kwargs) -> bool:
        return True

    async def send_as_bot(bot_id: int, *_args) -> bool:
        sent.append(bot_id)
        return True

    async def claim_completion(**kwargs) -> bool:
        return kwargs["bot_id"] == 300

    async def sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(shard_count.shard_ctx, "sharding_active", lambda: False)
    monkeypatch.setattr(shard_count, "list_local_fleet_bots_in_group", list_local_bots)
    monkeypatch.setattr(shard_count, "resolve_local_connected_bots_in_group", resolve_local_bots)
    monkeypatch.setattr(shard_count, "run_shard_coordinated_bot_count", run_coord)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", update_registration)
    monkeypatch.setattr(shard_count, "get_shard_bot_count_order", read_order)
    monkeypatch.setattr(shard_count, "wait_shard_bot_count_turn", wait_turn)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)
    monkeypatch.setattr(shard_count, "mark_shard_bot_count_reported_and_claim_completion", claim_completion)
    monkeypatch.setattr(shard_count.asyncio, "sleep", sleep)

    await shard_count.run_shard_bot_count(Bot(), Event())

    assert sent == [100, 100, 300, 300]
    assert notices == []


@pytest.mark.asyncio
async def test_unified_handler_registers_before_reading_coordination_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pallas_plugin_bot_status import shard_count

    steps: list[str] = []

    class Bot:
        self_id = "100"

    class Event:
        group_id = 10086
        user_id = 20001
        time = 30002

        def get_plaintext(self) -> str:
            return "牛牛报数"

    async def local_bots(_group_id: int) -> list[int]:
        return [100]

    async def register(**_kwargs) -> None:
        steps.append("register-start")
        await asyncio.sleep(0)
        steps.append("register-done")

    async def run_coord(**_kwargs) -> tuple[int, int]:
        steps.append("coord")
        assert steps.index("register-done") < steps.index("coord")
        return 1, 1

    async def read_order(**_kwargs) -> list[int]:
        return [100]

    async def wait_turn(**_kwargs) -> bool:
        return True

    async def send_as_bot(*_args) -> bool:
        return True

    async def claim_completion(**_kwargs) -> bool:
        return True

    monkeypatch.setattr(shard_count.shard_ctx, "sharding_active", lambda: False)
    monkeypatch.setattr(shard_count, "list_local_fleet_bots_in_group", local_bots)
    monkeypatch.setattr(shard_count, "resolve_local_connected_bots_in_group", local_bots)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", register)
    monkeypatch.setattr(shard_count, "run_shard_coordinated_bot_count", run_coord)
    monkeypatch.setattr(shard_count, "get_shard_bot_count_order", read_order)
    monkeypatch.setattr(shard_count, "wait_shard_bot_count_turn", wait_turn)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)
    monkeypatch.setattr(shard_count, "mark_shard_bot_count_reported_and_claim_completion", claim_completion)

    await shard_count.run_shard_bot_count(Bot(), Event())

    assert steps == ["register-start", "register-done", "coord"]


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

    async def wait_turn(**_kwargs) -> bool:
        return True

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
    monkeypatch.setattr(shard_count, "wait_shard_bot_count_turn", wait_turn)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)

    async def sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(shard_count, "mark_shard_bot_count_reported_and_claim_completion", claim_completion)
    monkeypatch.setattr(shard_count.asyncio, "sleep", sleep)

    await shard_count.run_shard_bot_count(Bot(), Event())

    assert completion_claims == []


@pytest.mark.asyncio
async def test_shard_mode_sends_collection_notice_once_from_first_reporter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pallas_plugin_bot_status import shard_count

    sent_as: list[tuple[int, str]] = []
    sent_group: list[str] = []

    class Bot:
        self_id = "200"

        async def send_group_msg(self, *, group_id: int, message: str) -> None:
            sent_group.append(message)

    class Event:
        group_id = 10086
        user_id = 20001
        time = 30002

        def get_plaintext(self) -> str:
            return "牛牛报数"

    async def run_coord(**_kwargs) -> tuple[int, int]:
        return 1, 3

    async def update_registration(**_kwargs) -> None:
        return None

    async def read_order(**_kwargs) -> list[int]:
        return [200, 300, 100]

    async def wait_turn(**_kwargs) -> bool:
        return True

    async def send_as_bot(bot_id: int, _group_id: int, message: str) -> bool:
        sent_as.append((bot_id, message))
        return True

    async def claim_completion(**_kwargs) -> bool:
        return True

    async def sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(shard_count.shard_ctx, "sharding_active", lambda: True)
    monkeypatch.setattr(shard_count.shard_ctx, "is_local_representative", lambda _bot_id: False)
    monkeypatch.setattr(shard_count, "run_shard_coordinated_bot_count", run_coord)
    monkeypatch.setattr(shard_count, "update_shard_bot_count_registration", update_registration)
    monkeypatch.setattr(shard_count, "get_shard_bot_count_order", read_order)
    monkeypatch.setattr(shard_count, "wait_shard_bot_count_turn", wait_turn)
    monkeypatch.setattr(shard_count, "send_group_message_as_bot", send_as_bot)
    monkeypatch.setattr(shard_count, "mark_shard_bot_count_reported_and_claim_completion", claim_completion)
    monkeypatch.setattr(shard_count.asyncio, "sleep", sleep)

    await shard_count.run_shard_bot_count(Bot(), Event())

    assert sent_as == [(200, "牛牛集合！")]
    assert sent_group == ["牛牛1号报到！", "牛牛们报数完毕！"]
