"""分片模式下的牛牛报数协调。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from nonebot import logger
from pallas.api.platform import (
    NS_LOCAL_CONNECTED,
    STAGGER_SEC,
    clear_group_online_cache,
    get_shard_bot_count_order,
    mark_shard_bot_count_reported_and_claim_completion,
    resolve_local_connected_bots_in_group,
    run_shard_coordinated_bot_count,
    send_group_message_as_bot,
    update_shard_bot_count_registration,
    wait_shard_bot_count_turn,
)
from pallas.api.platform_fleet_probe import list_local_fleet_bots_in_group
from pallas.core.platform.shard import context as shard_ctx

if TYPE_CHECKING:
    from nonebot.adapters.onebot.v11 import Bot, GroupMessageEvent


BotCountWork = Callable[[], Awaitable[None]]
bot_count_tasks: dict[int, asyncio.Task[None]] = {}


def start_background_bot_count(group_id: int, work: BotCountWork) -> bool:
    """同一部署内每群只允许一个报数任务，避免新一轮插入当前报数。"""
    current = bot_count_tasks.get(group_id)
    if current is not None and not current.done():
        return False

    task = asyncio.create_task(work(), name=f"bot_count:{group_id}")
    bot_count_tasks[group_id] = task

    def clear_completed_task(done_task: asyncio.Task[None]) -> None:
        if bot_count_tasks.get(group_id) is done_task:
            bot_count_tasks.pop(group_id, None)
        if done_task.cancelled():
            return
        try:
            done_task.result()
        except Exception as exc:
            logger.exception("bot_count: background task failed group={}: {}", group_id, exc)

    task.add_done_callback(clear_completed_task)
    return True


async def handle_shard_bot_count(
    bot: Bot,
    event: GroupMessageEvent,
) -> bool:
    """启动后台协调报数，使 matcher 及时释放同群会话队列。"""
    current = bot_count_tasks.get(event.group_id)
    if current is not None and not current.done():
        await update_shard_bot_count_registration(
            group_id=event.group_id,
            user_id=int(event.user_id),
            plaintext=(event.get_plaintext() or "").strip(),
            message_time=event.time,
            bot_ids=[int(bot.self_id)],
        )
        return False
    return start_background_bot_count(event.group_id, lambda: run_shard_bot_count(bot, event))


async def run_shard_bot_count(bot: Bot, event: GroupMessageEvent) -> None:
    self_id = int(bot.self_id)
    plain = (event.get_plaintext() or "").strip()
    local_ids = [self_id]
    unified = not shard_ctx.sharding_active()
    if unified or shard_ctx.is_local_representative(self_id):
        probed = await list_local_fleet_bots_in_group(event.group_id)
        local_ids = sorted({self_id, *probed})
    if unified:
        clear_group_online_cache(NS_LOCAL_CONNECTED)
        connected = await resolve_local_connected_bots_in_group(event.group_id)
        local_ids = sorted({*local_ids, *connected})

    if (unified or shard_ctx.is_local_representative(self_id)) and local_ids:
        await update_shard_bot_count_registration(
            group_id=event.group_id,
            user_id=int(event.user_id),
            plaintext=plain,
            message_time=event.time,
            bot_ids=local_ids,
        )
    coord = await run_shard_coordinated_bot_count(
        group_id=event.group_id,
        user_id=int(event.user_id),
        plaintext=plain,
        message_time=event.time,
        self_bot_id=self_id,
        local_bot_ids=local_ids,
    )
    if coord is None:
        return
    if unified:
        order = await get_shard_bot_count_order(
            group_id=event.group_id,
            user_id=int(event.user_id),
            plaintext=plain,
            message_time=event.time,
        )
        if not order:
            return
        if order[0] == self_id:
            try:
                await bot.send_group_msg(group_id=event.group_id, message="牛牛集合！")
            except Exception as e:
                logger.warning(f"bot [{self_id}] shard bot_count notice failed in group [{event.group_id}]: {e}")
        local_ids_set = set(local_ids)
        last_sent_bot_id: int | None = None
        last_sent_index = 0
        dispatch_started_at = time.monotonic()
        for index, bot_id in enumerate(order, start=1):
            if bot_id not in local_ids_set:
                continue
            turn_ready = await wait_shard_bot_count_turn(
                group_id=event.group_id,
                user_id=int(event.user_id),
                plaintext=plain,
                message_time=event.time,
                bot_id=bot_id,
                allow_timeout=False,
            )
            if not turn_ready:
                logger.debug(
                    "bot_count: turn timeout group={} bot={} index={}",
                    event.group_id,
                    bot_id,
                    index,
                )
            delay = (index - 1) * STAGGER_SEC - (time.monotonic() - dispatch_started_at)
            await asyncio.sleep(max(0.0, delay))
            try:
                sent = await send_group_message_as_bot(bot_id, event.group_id, f"牛牛{index}号报到！")
            except Exception as e:
                logger.warning(f"bot [{bot_id}] shard bot_count send failed in group [{event.group_id}]: {e}")
                continue
            if not sent:
                logger.warning(f"bot [{bot_id}] shard bot_count send was rejected in group [{event.group_id}]")
                continue
            last_sent_bot_id = bot_id
            last_sent_index = index
            if await mark_shard_bot_count_reported_and_claim_completion(
                group_id=event.group_id,
                user_id=int(event.user_id),
                plaintext=plain,
                message_time=event.time,
                bot_id=bot_id,
                allow_timeout=False,
            ):
                await asyncio.sleep(0.3)
                await send_group_message_as_bot(last_sent_bot_id, event.group_id, "牛牛们报数完毕！")
                return
        if last_sent_bot_id is not None:
            await asyncio.sleep((len(order) - last_sent_index) * STAGGER_SEC + 0.8)
            if await mark_shard_bot_count_reported_and_claim_completion(
                group_id=event.group_id,
                user_id=int(event.user_id),
                plaintext=plain,
                message_time=event.time,
                bot_id=last_sent_bot_id,
                allow_timeout=True,
            ):
                await asyncio.sleep(0.3)
                await send_group_message_as_bot(last_sent_bot_id, event.group_id, "牛牛们报数完毕！")
        return
    index, total = coord
    await asyncio.sleep((index - 1) * STAGGER_SEC)
    try:
        await bot.send_group_msg(group_id=event.group_id, message=f"牛牛{index}号报到！")
    except Exception as e:
        logger.warning(f"bot [{self_id}] shard bot_count send failed in group [{event.group_id}]: {e}")
        return
    claimed_completion = await mark_shard_bot_count_reported_and_claim_completion(
        group_id=event.group_id,
        user_id=int(event.user_id),
        plaintext=plain,
        message_time=event.time,
        bot_id=self_id,
        allow_timeout=False,
    )
    if not claimed_completion:
        await asyncio.sleep((total - index) * STAGGER_SEC + 0.8)
        claimed_completion = await mark_shard_bot_count_reported_and_claim_completion(
            group_id=event.group_id,
            user_id=int(event.user_id),
            plaintext=plain,
            message_time=event.time,
            bot_id=self_id,
            allow_timeout=True,
        )
    if claimed_completion:
        await asyncio.sleep(0.3)
        try:
            await bot.send_group_msg(group_id=event.group_id, message="牛牛们报数完毕！")
        except Exception as e:
            logger.warning(f"bot [{self_id}] shard bot_count completion failed in group [{event.group_id}]: {e}")
