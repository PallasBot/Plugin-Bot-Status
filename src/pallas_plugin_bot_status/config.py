from typing import Literal

from pallas.api.config import install_hot_reload_config
from pydantic import BaseModel, Field

StatusListModeSetting = Literal["auto", "session", "fleet", "connected"]


class Config(BaseModel, extra="ignore"):
    bot_status_notice_email: str = Field(default="", description="接收 Bot 状态告警（如掉线）的收件人邮箱。")
    bot_status_offline_grace_time: int = Field(
        default=30,
        description="判定为离线并发送邮件通知前，允许无心跳的宽限时间（分钟）。",
    )
    bot_status_list_mode: StatusListModeSetting = Field(
        default="auto",
        description=(
            "牛牛在吗名册：auto=分片用协议登记、单进程用本机连接；"
            "session=仅本进程已连接；"
            "fleet=协议端已启用且登记；"
            "connected=全集群曾上线（不含仅登记未连过的号）。"
        ),
    )
    bot_status_show_other_deployments: bool = Field(
        default=True,
        description="牛牛在吗是否展示联邦协同池中的其他部署；关闭后只展示当前部署。",
    )


plugin_webui = install_hot_reload_config(Config, config_module=__name__)
get_bot_status_config = plugin_webui.get
reload_bot_status_config = plugin_webui.reload
clear_bot_status_config_cache = plugin_webui.clear_cache
