import asyncio
from dataclasses import dataclass
from datetime import datetime

from nonebot import logger
from nonebot.adapters.onebot.v11 import GroupMessageEvent, Message, MessageEvent
from nonebot.matcher import Matcher
from nonebot.params import CommandArg
from pallas.api.config import get_bot_admins
from pallas.api.logging import format_plugin_event
from pallas.api.utils import build_mail_config, get_smtp_config, send_mail

from .bot_monitor import get_bot_status_info
from .config import get_bot_status_config

STATUS_COOLDOWN_KEY: str = "bot_status"
OFFLINE_MAIL_COOLDOWN_KEY: str = "bot_status.offline_mail"
OFFLINE_MAIL_SEND_INTERVAL_SEC = 2.5
OFFLINE_MAIL_RETRY_COUNT = 2
OFFLINE_MAIL_RETRY_DELAY_SEC = 5.0


@dataclass(frozen=True)
class OwnerNotifyResult:
    bot_id: int
    nickname: str
    sent: int
    failed: int
    skipped_no_admins: bool
    errors: tuple[str, ...]
    notified_owner_ids: tuple[int, ...]


def smtp_transport_ready() -> bool:
    smtp = get_smtp_config()
    return bool(smtp.smtp_user and smtp.smtp_password and smtp.smtp_server and smtp.smtp_port)


def offline_mail_content(bot_id: int, nickname: str, offline_reason: str = "") -> tuple[str, str]:
    title = f"[牛牛不见啦] {nickname} 已离线 "
    reason_info = f"离线原因: {offline_reason}" if offline_reason else ""
    content = f"""
{reason_info}

牛牛昵称：{nickname}
牛牛账号：{bot_id}
掉线时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    """.strip()
    return title, content


def batch_offline_mail_content(bots: list[tuple[int, str]]) -> tuple[str, str]:
    lines = "\n".join(f"- {nickname}（{bot_id}）" for bot_id, nickname in bots)
    title = f"[牛牛不见啦] {len(bots)} 头牛牛已离线"
    content = f"""
以下牛牛当前离线：

{lines}

通知时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    """.strip()
    return title, content


def is_transient_mail_error(message: str) -> bool:
    lowered = message.lower()
    return any(token in lowered for token in ("connection lost", "timeout", "421", "rate", "too many"))


async def send_mail_with_retry(
    title: str,
    content: str,
    owner_id: int,
) -> str | None:
    email = f"{owner_id}@qq.com"
    mail_config = build_mail_config(email)
    last_error: str | None = None
    for attempt in range(OFFLINE_MAIL_RETRY_COUNT + 1):
        last_error = await send_mail(title, content, mail_config)
        if last_error is None:
            return None
        if attempt < OFFLINE_MAIL_RETRY_COUNT and is_transient_mail_error(last_error):
            logger.warning(
                f"bot_status offline mail to owner [{email}] retry {attempt + 1}/"
                f"{OFFLINE_MAIL_RETRY_COUNT}: {last_error}"
            )
            await asyncio.sleep(OFFLINE_MAIL_RETRY_DELAY_SEC)
            continue
        return last_error
    return last_error


async def get_bot_admin_emails(bot_id: int) -> list[str]:
    """获取牛牛的admins邮箱列表"""
    emails: list[str] = []

    try:
        admins = await get_bot_admins(bot_id)
        if admins:
            emails.extend(f"{admin_id}@qq.com" for admin_id in admins)
    except Exception as e:
        logger.debug(f"bot [{bot_id}] bot_status get_bot_admin_emails failed: {e}")

    return emails


async def notify_bot_offline_to_owners(
    bot_id: int,
    nickname: str,
    offline_reason: str = "",
) -> OwnerNotifyResult:
    """仅向牛牛号主（BotConfig.admins）发送离线提醒。"""
    if not smtp_transport_ready():
        return OwnerNotifyResult(
            bot_id=bot_id,
            nickname=nickname,
            sent=0,
            failed=0,
            skipped_no_admins=False,
            errors=("SMTP 配置不完整",),
            notified_owner_ids=(),
        )

    try:
        owner_ids = await get_bot_admins(bot_id)
    except Exception as e:
        logger.debug(f"bot [{bot_id}] bot_status get_bot_admins failed: {e}")
        owner_ids = []

    if not owner_ids:
        return OwnerNotifyResult(
            bot_id=bot_id,
            nickname=nickname,
            sent=0,
            failed=0,
            skipped_no_admins=True,
            errors=(),
            notified_owner_ids=(),
        )

    title, content = offline_mail_content(bot_id, nickname, offline_reason)
    sent = 0
    failed = 0
    errors: list[str] = []
    notified_owner_ids: list[int] = []
    for owner_id in owner_ids:
        result = await send_mail_with_retry(title, content, owner_id)
        if result:
            failed += 1
            errors.append(f"号主 {owner_id}: {result}")
            logger.error(f"bot [{bot_id}] offline mail to owner [{owner_id}@qq.com] failed: {result}")
        else:
            sent += 1
            notified_owner_ids.append(owner_id)
            logger.info(
                format_plugin_event(
                    "offline_mail",
                    f"Bot [{bot_id}] sent an offline notification mail to owner [{owner_id}@qq.com]",
                )
            )
        if len(owner_ids) > 1:
            await asyncio.sleep(OFFLINE_MAIL_SEND_INTERVAL_SEC)

    return OwnerNotifyResult(
        bot_id=bot_id,
        nickname=nickname,
        sent=sent,
        failed=failed,
        skipped_no_admins=False,
        errors=tuple(errors),
        notified_owner_ids=tuple(notified_owner_ids),
    )


async def notify_bot_offline(bot_id: int, nickname: str, offline_reason: str = "") -> None:
    """通知牛牛离线"""
    cfg = get_bot_status_config()
    admin_emails: list[str] = await get_bot_admin_emails(bot_id)

    mail_config = build_mail_config(cfg.bot_status_notice_email)

    if mail_config.check_params():
        title, content = offline_mail_content(bot_id, nickname, offline_reason)

        result: str | None = await send_mail(title, content, mail_config)
        if result:
            logger.error(f"bot [{bot_id}] offline notification mail failed: {result}")
        else:
            logger.info(
                format_plugin_event(
                    "offline_mail",
                    f"Bot [{bot_id}] sent an offline notification mail (notice_email)",
                )
            )

        for email in admin_emails:
            try:
                admin_mail_config = build_mail_config(email)
                result = await send_mail(title, content, admin_mail_config)
                if result:
                    logger.error(f"bot [{bot_id}] offline mail to admin [{email}] failed: {result}")
                else:
                    logger.info(
                        format_plugin_event(
                            "offline_mail",
                            f"Bot [{bot_id}] sent an offline notification mail to admin [{email}]",
                        )
                    )
            except Exception as e:
                logger.error(f"bot [{bot_id}] offline mail to admin [{email}] exception: {e}")
    else:
        logger.warning("bot_status mail skipped: SMTP config incomplete")


def parse_offline_mail_target(args: Message) -> int | None:
    plain = args.extract_plain_text().strip()
    if not plain:
        return None
    token = plain.split()[0]
    if token.isdigit():
        return int(token)
    return None


def offline_bot_label(nickname: str, bot_id: int) -> str:
    return f"{nickname}（{bot_id}）"


def owner_ids_label(owner_ids: tuple[int, ...]) -> str:
    return "、".join(str(owner_id) for owner_id in owner_ids)


def format_owner_notify_summary(results: list[OwnerNotifyResult]) -> str:
    sent_lines: list[str] = []
    no_admin: list[str] = []
    failed: list[str] = []

    for item in results:
        bot_label = offline_bot_label(item.nickname, item.bot_id)
        if item.skipped_no_admins:
            no_admin.append(bot_label)
        elif item.sent > 0 and item.failed == 0:
            sent_lines.append(f"{bot_label} → 号主 {owner_ids_label(item.notified_owner_ids)}")
        elif item.sent > 0:
            sent_lines.append(f"{bot_label} → 号主 {owner_ids_label(item.notified_owner_ids)}")
            failed.append(f"{bot_label}: {item.errors[0] if item.errors else '部分号主发送失败'}")
        elif item.errors:
            failed.append(f"{bot_label}: {item.errors[0]}")
        else:
            failed.append(bot_label)

    parts: list[str] = []
    if sent_lines:
        parts.append("已通知号主：\n" + "\n".join(sent_lines))
    if no_admin:
        parts.append(f"未配置号主，已跳过牛牛：{'、'.join(no_admin)}")
    if failed:
        parts.append(f"发送失败：{'；'.join(failed)}")
    return "\n".join(parts) if parts else "未发送任何邮件"


def empty_owner_notify_result(bot_id: int, nickname: str) -> OwnerNotifyResult:
    return OwnerNotifyResult(
        bot_id=bot_id,
        nickname=nickname,
        sent=0,
        failed=0,
        skipped_no_admins=False,
        errors=(),
        notified_owner_ids=(),
    )


async def send_batch_offline_mails_by_owner(
    targets: dict[int, str],
) -> list[OwnerNotifyResult]:
    """批量离线提醒：同一号主合并为一封，发信间隔并带重试。"""
    results = {bot_id: empty_owner_notify_result(bot_id, nickname) for bot_id, nickname in targets.items()}
    owner_bots: dict[int, list[tuple[int, str]]] = {}

    for bot_id, nickname in targets.items():
        try:
            owner_ids = await get_bot_admins(bot_id)
        except Exception as e:
            logger.debug(f"bot [{bot_id}] bot_status get_bot_admins failed: {e}")
            owner_ids = []
        if not owner_ids:
            results[bot_id] = OwnerNotifyResult(
                bot_id=bot_id,
                nickname=nickname,
                sent=0,
                failed=0,
                skipped_no_admins=True,
                errors=(),
                notified_owner_ids=(),
            )
            continue
        for owner_id in owner_ids:
            owner_bots.setdefault(owner_id, []).append((bot_id, nickname))

    for owner_id, bots in owner_bots.items():
        deduped: list[tuple[int, str]] = []
        seen: set[int] = set()
        for bot_id, nickname in bots:
            if bot_id in seen:
                continue
            seen.add(bot_id)
            deduped.append((bot_id, nickname))

        if len(deduped) == 1:
            title, content = offline_mail_content(deduped[0][0], deduped[0][1])
        else:
            title, content = batch_offline_mail_content(deduped)

        error = await send_mail_with_retry(title, content, owner_id)
        if error:
            for bot_id, nickname in deduped:
                current = results[bot_id]
                results[bot_id] = OwnerNotifyResult(
                    bot_id=bot_id,
                    nickname=nickname,
                    sent=current.sent,
                    failed=current.failed + 1,
                    skipped_no_admins=False,
                    errors=current.errors + (f"号主 {owner_id}: {error}",),
                    notified_owner_ids=current.notified_owner_ids,
                )
        else:
            for bot_id, nickname in deduped:
                current = results[bot_id]
                notified = tuple(dict.fromkeys((*current.notified_owner_ids, owner_id)))
                results[bot_id] = OwnerNotifyResult(
                    bot_id=bot_id,
                    nickname=nickname,
                    sent=current.sent + 1,
                    failed=current.failed,
                    skipped_no_admins=False,
                    errors=current.errors,
                    notified_owner_ids=notified,
                )

        await asyncio.sleep(OFFLINE_MAIL_SEND_INTERVAL_SEC)

    return list(results.values())


async def handle_offline_mail_command(
    bot,
    event: MessageEvent,
    args: Message = CommandArg(),  # noqa: B008
) -> None:
    """超管手动向离线牛牛号主发送提醒邮件。"""
    from pallas.api.limits import is_command_cooldown_ready, refresh_command_cooldown

    if isinstance(event, GroupMessageEvent):
        if not await is_command_cooldown_ready(event, OFFLINE_MAIL_COOLDOWN_KEY):
            return
        await refresh_command_cooldown(event, OFFLINE_MAIL_COOLDOWN_KEY)

    matcher = Matcher()
    if not smtp_transport_ready():
        smtp = get_smtp_config()
        missing: list[str] = []
        if not smtp.smtp_user:
            missing.append("PALLAS_SMTP_USER")
        if not smtp.smtp_password:
            missing.append("PALLAS_SMTP_PASSWORD")
        if not smtp.smtp_server:
            missing.append("PALLAS_SMTP_SERVER")
        if not smtp.smtp_port:
            missing.append("PALLAS_SMTP_PORT")
        await matcher.finish(f"SMTP 配置不完整，缺少: {', '.join(missing)}")

    _, offline_bots = await get_bot_status_info()
    target_id = parse_offline_mail_target(args)

    if target_id is not None:
        nickname = offline_bots.get(target_id)
        if nickname is None:
            await matcher.finish(f"牛牛 {target_id} 当前不在离线名册中")
        targets = {target_id: nickname}
    else:
        targets = offline_bots

    if not targets:
        await matcher.finish("当前没有离线牛牛")

    if len(targets) == 1:
        bot_id, nickname = next(iter(targets.items()))
        results = [await notify_bot_offline_to_owners(bot_id, nickname)]
    else:
        await bot.send(
            event,
            "开始发送离线提醒邮件，号主相同时会合并为一封，请稍候…",
        )
        results = await send_batch_offline_mails_by_owner(targets)

    await matcher.finish(format_owner_notify_summary(results))


async def handle_test_mail_command(bot, event) -> None:
    """处理测试邮件命令"""
    from nonebot.adapters.onebot.v11 import GroupMessageEvent
    from nonebot.matcher import Matcher

    if isinstance(event, GroupMessageEvent):
        from pallas.api.config import GroupConfig

        config = GroupConfig(group_id=event.group_id, cooldown=10)
        if not await config.is_cooldown(STATUS_COOLDOWN_KEY):
            return
        await config.refresh_cooldown(STATUS_COOLDOWN_KEY)

    cfg = get_bot_status_config()
    smtp = get_smtp_config()
    mail_config = build_mail_config(cfg.bot_status_notice_email)

    if not mail_config.check_params():
        missing_params: list[str] = []
        if not smtp.smtp_user:
            missing_params.append("bot_status_smtp_user")
        if not smtp.smtp_password:
            missing_params.append("bot_status_smtp_password")
        if not smtp.smtp_server:
            missing_params.append("bot_status_smtp_server")
        if not cfg.bot_status_notice_email:
            missing_params.append("bot_status_notice_email")

        matcher = Matcher()
        await matcher.finish(f"邮箱配置缺少参数: {', '.join(missing_params)}")
        return

    title: str = "[Test]  这是一封测试邮件"
    content: str = f"""
牛牛在吗？

发送时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Bot ID: {bot.self_id}

如果你收到了这封邮件，证明邮箱配置正确。
    """.strip()

    result: str | None = await send_mail(title, content, mail_config)
    matcher = Matcher()
    if result:
        await matcher.finish(f"测试邮件发送失败: {result}")
    else:
        await matcher.finish("测试邮件发送成功！")
