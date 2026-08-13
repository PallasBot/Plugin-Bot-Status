import asyncio
from datetime import datetime, timedelta

from nonebot import get_bots, logger
from nonebot.adapters.onebot.v11 import Bot
from nonebot_plugin_apscheduler import scheduler
from pallas.api.logging import format_plugin_event

from .config import get_bot_status_config
from .list_mode import (
    cluster_online_bot_ids_for_status,
    resolve_status_list_mode,
    status_inventory_bot_ids,
)

offline_bots: dict[int, dict[str, str]] = {}

STATUS_COOLDOWN_KEY: str = "bot_status"


def shutdown_in_progress() -> bool:
    """进程正在收尾（Ctrl+C 等）时，跳过断连处理，避免刷屏与拖慢退出。"""
    try:
        from pallas.api.platform import is_process_shutting_down
    except Exception:
        return False
    try:
        return bool(is_process_shutting_down())
    except Exception:
        return False


def protocol_offline_marked(bot_id: int) -> bool:
    """协议端/断线流程已记入 offline_bots（含宽限待确认）。"""
    rec = offline_bots.get(bot_id)
    return isinstance(rec, dict) and bool(rec.get("source"))


def cluster_online_bot_ids(current_bots: dict | None = None) -> set[int]:
    """在线集合。"""
    return cluster_online_bot_ids_for_status(current_bots)


_NICKNAME_QUERY_TIMEOUT_SEC = 3.0


def _nickname_query_timeout_sec() -> float:
    from .config import get_bot_status_config

    try:
        return float(get_bot_status_config().bot_status_nickname_query_timeout_sec or _NICKNAME_QUERY_TIMEOUT_SEC)
    except Exception:
        return _NICKNAME_QUERY_TIMEOUT_SEC


async def _query_stranger_info(bot_instance, user_id: int) -> dict:
    """带超时查询陌生人信息，避免对离线/无响应 bot 卡住整个状态查询。"""
    return await asyncio.wait_for(
        bot_instance.call_api("get_stranger_info", user_id=user_id),
        timeout=_nickname_query_timeout_sec(),
    )


async def get_bot_nickname(bot_id: int, current_bots: dict = None) -> str:
    """获取牛牛昵称"""
    nickname: str = "Unknown Nickname"
    try:
        bots = current_bots if current_bots is not None else get_bots()

        # 首先尝试让牛牛自己获取自己的信息
        if str(bot_id) in bots:
            try:
                info = await _query_stranger_info(bots[str(bot_id)], bot_id)
                nickname = info.get("nickname", "Unknown Nickname")
                if nickname != "Unknown Nickname":
                    return nickname
            except Exception as e:
                logger.debug(f"bot [{bot_id}] get_stranger_info via self failed: {e}")

        available_bots = [bot_instance for bot_id_key, bot_instance in bots.items() if int(bot_id_key) != bot_id]

        max_retries = 3
        for attempt in range(max_retries):
            if attempt > 0:
                logger.debug(f"bot [{bot_id}] nickname lookup retry {attempt + 1}/{max_retries}")

            for bot_instance in available_bots:
                try:
                    info = await _query_stranger_info(bot_instance, bot_id)
                    nickname = info.get("nickname", "Unknown Nickname")
                    if nickname != "Unknown Nickname":
                        return nickname
                except Exception as e:
                    logger.debug(
                        f"bot [{bot_id}] get_stranger_info via bot [{bot_instance.self_id}] "
                        f"attempt {attempt + 1} failed: {e}"
                    )
                    continue

            if attempt < max_retries - 1:
                await asyncio.sleep(0.1)

    except Exception as e:
        logger.debug(f"bot [{bot_id}] get_nickname failed: {e}")

    return nickname


async def handle_bot_connect(bot: Bot) -> None:
    bot_id: int = int(bot.self_id)
    was_offline = bot_id in offline_bots
    offline_bots.pop(bot_id, None)
    if was_offline:
        logger.info(
            format_plugin_event(
                "bot_online",
                f"Bot [{bot_id}] reconnected and cleared offline mark",
            )
        )


async def handle_bot_disconnect(bot: Bot) -> None:
    bot_id: int = int(bot.self_id)
    if shutdown_in_progress():
        logger.debug(f"bot [{bot_id}] skipped disconnect handling during shutdown")
        return
    if bot_id in offline_bots and "source" in offline_bots[bot_id]:
        # 已经处理过了，直接返回
        return

    nickname: str = await get_bot_nickname(bot_id)

    offline_bots[bot_id] = {
        "nickname": nickname,
        "offline_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "disconnect_event",
    }

    job_id: str = f"bot_status_check_{bot_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    # 计算运行时间
    grace_sec = get_bot_status_config().bot_status_offline_grace_time
    run_time: datetime = datetime.now() + timedelta(seconds=grace_sec)

    logger.info(
        format_plugin_event(
            "bot_disconnect",
            f"Bot [{bot_id}] disconnected, will re-check after [{grace_sec}]s grace",
        )
    )

    scheduler.add_job(
        id=job_id,
        func=check_bot_still_offline,
        args=[
            bot_id,
            nickname,
        ],
        misfire_grace_time=60,
        coalesce=True,
        max_instances=1,
        trigger="date",
        run_date=run_time,
    )


async def check_bot_still_offline(bot_id: int, nickname: str) -> None:
    """检查牛牛是否真的离线"""
    if shutdown_in_progress():
        return
    if bot_id in cluster_online_bot_ids():
        if (
            bot_id in offline_bots
            and "source" in offline_bots[bot_id]
            and offline_bots[bot_id]["source"] == "checked_offline"
        ):
            del offline_bots[bot_id]
        return

    bots = get_bots()
    if str(bot_id) not in bots:
        logger.warning(
            format_plugin_event(
                "bot_offline",
                f"Bot [{bot_id}] went offline after grace and sent notification",
            )
        )
        # 更新离线时间
        if bot_id in offline_bots:
            offline_bots[bot_id]["offline_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            # 如果不在离线列表中，则添加进去
            offline_bots[bot_id] = {
                "nickname": nickname,
                "offline_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "checked_offline",
            }
        # 发送离线通知
        try:
            from .mail_notifier import notify_bot_offline

            await notify_bot_offline(bot_id, nickname)
        except Exception as e:
            logger.error(f"牛牛 [{bot_id}] 离线通知处理失败：{e}")
    else:
        # 牛牛实际上在线，从离线列表中删除
        if (
            bot_id in offline_bots
            and "source" in offline_bots[bot_id]
            and offline_bots[bot_id]["source"] == "checked_offline"
        ):
            del offline_bots[bot_id]
        elif bot_id in offline_bots and "source" not in offline_bots[bot_id]:
            pass


async def list_connected_bots_in_group(group_id: int) -> list[int]:
    """本进程已连接且能查到该群成员资料的牛牛 QQ。"""
    from pallas.api.platform import (
        resolve_local_connected_bots_in_group,
    )

    return await resolve_local_connected_bots_in_group(group_id)


async def get_bot_status_info() -> tuple[dict[int, str], dict[int, str]]:
    """获取牛牛状态信息"""
    import time

    started = time.monotonic()

    # 获取当前在线的牛牛
    current_bots = get_bots()

    all_bot_ids = set(status_inventory_bot_ids())
    all_bot_ids.update(int(bot_id) for bot_id in current_bots.keys())
    if resolve_status_list_mode() != "connected":
        all_bot_ids.update(offline_bots.keys())

    online_ids = cluster_online_bot_ids(current_bots)

    async def get_nickname_with_status(bot_id: int) -> tuple[int, str, bool]:
        """获取昵称和在线状态任务"""
        # NapCat/Lagrange 掉线或宽限判离线后，优先于僵尸 WS / 滞后 presence。
        # 已标记离线的 bot 不再尝试实时查昵称，避免对协议端超时查询卡住整条命令。
        if protocol_offline_marked(bot_id):
            nickname = offline_bots[bot_id].get("nickname", "Unknown Nickname")
            return bot_id, nickname, False
        nickname = await get_bot_nickname(bot_id, current_bots)
        if bot_id in online_ids:
            return bot_id, nickname, True
        if bot_id in offline_bots:
            offline_bots[bot_id]["nickname"] = nickname
        return bot_id, nickname, False

    bot_info_tasks = [get_nickname_with_status(bot_id) for bot_id in all_bot_ids]
    bot_info_results = await asyncio.gather(*bot_info_tasks, return_exceptions=True)

    online_bots: dict[int, str] = {}
    offline_bots_filtered: dict[int, str] = {}

    for result in bot_info_results:
        if isinstance(result, Exception):
            logger.warning(f"bot_status get_bot_status_info task failed: {result}")
            continue

        bot_id, nickname, is_online = result
        if is_online:
            online_bots[bot_id] = nickname
            # 如果这个Bot之前在离线列表中，更新其昵称
            if bot_id in offline_bots:
                offline_bots[bot_id]["nickname"] = nickname
        else:
            offline_bots_filtered[bot_id] = nickname

    logger.info(
        format_plugin_event(
            "status_collected",
            (
                f"Bot status collected: {len(online_bots)} online, {len(offline_bots_filtered)} offline, "
                f"nickname query took {(time.monotonic() - started) * 1000:.0f}ms"
            ),
        )
    )
    return online_bots, offline_bots_filtered
