"""牛牛在吗：名册与在线判定模式。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from nonebot import get_bots
from pallas.api.platform import (
    connected_bot_ids,
    get_fleet_bot_ids,
    get_session_seen_bot_ids,
)
from pallas.core.platform.shard import context as shard_ctx

from .config import get_bot_status_config

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pallas.api.platform import FederatePeerBotRoster

StatusListMode = Literal["session", "fleet", "connected"]
ResolvedListMode = StatusListMode


def resolve_status_list_mode() -> ResolvedListMode:
    raw = (get_bot_status_config().bot_status_list_mode or "auto").strip().lower()
    if raw == "auto":
        return "fleet" if shard_ctx.sharding_active() else "session"
    if raw in ("session", "fleet", "connected"):
        return raw  # type: ignore[return-value]
    return "fleet" if shard_ctx.sharding_active() else "session"


def status_inventory_bot_ids(*, list_mode: ResolvedListMode | None = None) -> frozenset[int]:
    """名册 QQ：session=本进程；connected=曾上线；fleet=协议端已启用。"""
    mode = list_mode or resolve_status_list_mode()
    if mode == "fleet":
        return get_fleet_bot_ids()
    if mode == "connected":
        return get_session_seen_bot_ids()
    try:
        ids = {int(x) for x in connected_bot_ids()}
    except Exception:
        ids = set()
    if shard_ctx.sharding_active():
        for key in get_bots():
            try:
                ids.add(int(key))
            except ValueError:
                continue
    return frozenset(ids)


def cluster_online_bot_ids_for_status(
    current_bots: dict | None = None,
    *,
    list_mode: ResolvedListMode | None = None,
) -> set[int]:
    """在线集合：分片且 fleet/connected 时用集群在线表；否则用本进程 get_bots。"""
    from nonebot import get_bots as nb_get_bots

    mode = list_mode or resolve_status_list_mode()
    bots = current_bots if current_bots is not None else nb_get_bots()
    if shard_ctx.sharding_active() and mode in ("fleet", "connected"):
        from pallas.api.platform import get_cluster_online_bot_ids

        return set(get_cluster_online_bot_ids())
    out: set[int] = set()
    for key in bots:
        try:
            out.add(int(key))
        except ValueError:
            continue
    return out


def format_federate_status_rosters(
    rosters: Iterable[FederatePeerBotRoster],
    *,
    nicknames_by_id: dict[int, str] | None = None,
) -> str:
    """按部署展示协同池在线牛牛，QQ 仅展示对端允许公开的账号。"""
    nicknames = nicknames_by_id or {}
    sections: list[str] = []
    for roster in rosters:
        deployment_id = roster.deployment_id or "未知"
        name = roster.deployment_name or f"部署 {deployment_id[:7]}"
        online_ids = roster.online_bot_ids
        if online_ids is None:
            sections.append(f"{name}：状态未知（对端待升级）")
            continue
        if not online_ids:
            sections.append(f"{name}：暂无在线牛牛")
            continue
        public_ids = sorted(online_ids & roster.public_bot_ids)
        hidden_count = len(online_ids) - len(public_ids)
        lines = [f"{name}：{len(online_ids)}只在线"]
        if public_ids:
            for qq in public_ids:
                nickname = nicknames.get(qq, "").strip()
                lines.append(f"{nickname} ({qq})" if nickname else f"QQ {qq}")
        if hidden_count:
            lines.append(f"{'另有 ' if public_ids else ''}{hidden_count} 只在线未公开 QQ")
        sections.append("\n".join(lines))
    return "\n\n".join(sections)
