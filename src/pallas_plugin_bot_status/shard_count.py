"""分片模式下的牛牛报数协调。"""

from __future__ import annotations

import asyncio
import time
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
    from collections.abc import Awaitable, Callable

    from nonebot.adapters.onebot.v11 import Bot, GroupMessageEvent


async def handle_shard_bot_count(
    bot: Bot,
    event: GroupMessageEvent,
    *,
    finish: Callable[[str], Awaitable[None]],
) -> None:
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
        if self_id != min(local_ids):
            return
        order = await get_shard_bot_count_order(
            group_id=event.group_id,
            user_id=int(event.user_id),
            plaintext=plain,
            message_time=event.time,
        )
        if not order:
            return
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
            ):
                await asyncio.sleep(0.3)
                await finish("牛牛们报数完毕！")
                return
        if last_sent_bot_id is not None:
            await asyncio.sleep((len(order) - last_sent_index) * STAGGER_SEC + 0.8)
            if await mark_shard_bot_count_reported_and_claim_completion(
                group_id=event.group_id,
                user_id=int(event.user_id),
                plaintext=plain,
                message_time=event.time,
                bot_id=last_sent_bot_id,
            ):
                await asyncio.sleep(0.3)
                await finish("牛牛们报数完毕！")
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
    )
    if not claimed_completion:
        await asyncio.sleep((total - index) * STAGGER_SEC + 0.8)
        claimed_completion = await mark_shard_bot_count_reported_and_claim_completion(
            group_id=event.group_id,
            user_id=int(event.user_id),
            plaintext=plain,
            message_time=event.time,
            bot_id=self_id,
        )
    if claimed_completion:
        await asyncio.sleep(0.3)
        await finish("牛牛们报数完毕！")
