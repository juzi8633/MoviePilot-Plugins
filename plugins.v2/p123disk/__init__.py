from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional
import threading

from p123client import P123Client
from .p123_api import P123Api

from app import schemas
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType
from app.helper.storage import StorageHelper
from app.schemas import StorageOperSelectionEventData, FileItem


class P123AutoClient:
    """
    123云盘客户端 (线程安全版)
    """

    def __init__(self, passport, password):
        self._client = None
        self._passport = passport
        self._password = password
        self._lock = threading.Lock()  # 引入锁，防止多线程同时触发重连

    def __getattr__(self, name):
        if self._client is None:
            with self._lock:  # 双重检查锁定
                if self._client is None:
                    self._client = P123Client(self._passport, self._password)  # noqa

        def wrapped(*args, **kwargs):
            attr = getattr(self._client, name)
            if not callable(attr):
                return attr
            result = attr(*args, **kwargs)
            
            # 检查是否需要刷新Token
            if (
                isinstance(result, dict)
                and result.get("code") == 401
                and result.get("message") == "tokens number has exceeded the limit"
            ):
                # 获取锁，确保只有一个线程执行登录操作
                with self._lock:
                    # 再次检查，防止等待锁的过程中前一个线程已经修复了连接
                    # 这里做一个简单的重新实例化，虽然有点浪费但最安全
                    self._client = P123Client(self._passport, self._password)  # noqa
                
                # 获取新实例的方法
                attr = getattr(self._client, name)
                if not callable(attr):
                    return attr
                return attr(*args, **kwargs)
            return result

        return wrapped


class P123Disk(_PluginBase):
    # 插件名称
    plugin_name = "123云盘储存"
    # 插件描述
    plugin_desc = "使存储支持123云盘。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/juzi8633/MoviePilot-Plugins/main/icons/P123Disk.png"
    # 插件版本
    plugin_version = "1.2.4"
    # 插件作者
    plugin_author = "juzi8633"
    # 作者主页
    author_url = "https://github.com/juzi8633"
    # 插件配置项ID前缀
    plugin_config_prefix = "p123disk_"
    # 加载顺序
    plugin_order = 99
    # 可使用的用户级别
    auth_level = 1

    # 是否启用
    _enabled = False
    _client = None
    _disk_name = None
    _p123_api = None
    _passport = None
    _password = None
    
    # 新增配置项
    _webhook_url = None
    _webhook_secret = None
    _slice_size = None
    _upload_threads = 3

    def __init__(self):
        """
        初始化
        """
        super().__init__()

        self._disk_name = "123云盘"

    def init_plugin(self, config: dict = None):
        """
        初始化插件
        """
        if config:
            storage_helper = StorageHelper()
            storages = storage_helper.get_storagies()
            if not any(
                s.type == self._disk_name and s.name == self._disk_name
                for s in storages
            ):
                # 添加云盘存储配置
                storage_helper.add_storage(
                    storage=self._disk_name, name=self._disk_name, conf={}
                )

            self._enabled = config.get("enabled")
            self._passport = config.get("passport")
            self._password = config.get("password")
            
            # 读取新增配置
            self._webhook_url = config.get("webhook_url")
            self._webhook_secret = config.get("webhook_secret")
            self._slice_size = config.get("slice_size")
            self._upload_threads = config.get("upload_threads")

            try:
                self._client = P123AutoClient(self._passport, self._password)
                # 传递新参数给 API
                self._p123_api = P123Api(
                    client=self._client, 
                    disk_name=self._disk_name,
                    webhook_url=self._webhook_url,
                    webhook_secret=self._webhook_secret,
                    slice_size=self._slice_size,
                    upload_threads=self._upload_threads
                )  # noqa
            except Exception as e:
                logger.error(f"123云盘客户端创建失败: {e}")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "passport",
                                            "label": "手机号",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "password",
                                            "label": "密码",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    # Webhook, 分片, 线程配置
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "webhook_url",
                                            "label": "上传完成回调URL(POST)",
                                            "placeholder": "http://IP:Port/api/callback",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "webhook_secret",
                                            "label": "回调密钥(Secret)",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "slice_size",
                                            "label": "分片大小(MB)",
                                            "placeholder": "默认自动，建议 10-100",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "upload_threads",
                                            "label": "上传并发线程数",
                                            "placeholder": "默认为3，建议1-10",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "passport": "",
            "password": "",
            "webhook_url": "",
            "webhook_secret": "",
            "slice_size": "",
            "upload_threads": "3",
        }

    def get_page(self) -> List[dict]:
        pass

    def get_module(self) -> Dict[str, Any]:
        return {
            "list_files": self.list_files,
            "any_files": self.any_files,
            "download_file": self.download_file,
            "upload_file": self.upload_file,
            "delete_file": self.delete_file,
            "rename_file": self.rename_file,
            "get_file_item": self.get_file_item,
            "get_parent_item": self.get_parent_item,
            "snapshot_storage": self.snapshot_storage,
            "storage_usage": self.storage_usage,
            "support_transtype": self.support_transtype,
            "create_folder": self.create_folder,
            "exists": self.exists,
            "get_item": self.get_item,
        }

    @eventmanager.register(ChainEventType.StorageOperSelection)
    def storage_oper_selection(self, event: Event):
        if not self._enabled:
            return
        event_data: StorageOperSelectionEventData = event.event_data
        if event_data.storage == self._disk_name:
            event_data.storage_oper = self._p123_api  # noqa

    def list_files(
        self, fileitem: schemas.FileItem, recursion: bool = False
    ) -> Optional[List[schemas.FileItem]]:
        if fileitem.storage != self._disk_name:
            return None

        def __get_files(_item: FileItem, _r: Optional[bool] = False):
            _items = self._p123_api.list(_item)
            if _items:
                if _r:
                    for t in _items:
                        if t.type == "dir":
                            __get_files(t, _r)
                        else:
                            result.append(t)
                else:
                    result.extend(_items)

        result = []
        __get_files(fileitem, recursion)
        return result

    def any_files(
        self, fileitem: schemas.FileItem, extensions: list = None
    ) -> Optional[bool]:
        if fileitem.storage != self._disk_name:
            return None

        def __any_file(_item: FileItem):
            _items = self._p123_api.list(_item)
            if _items:
                if not extensions:
                    return True
                for t in _items:
                    if (
                        t.type == "file"
                        and t.extension
                        and f".{t.extension.lower()}" in extensions
                    ):
                        return True
                    elif t.type == "dir":
                        if __any_file(t):
                            return True
            return False

        return __any_file(fileitem)

    def create_folder(
        self, fileitem: schemas.FileItem, name: str
    ) -> Optional[schemas.FileItem]:
        if fileitem.storage != self._disk_name:
            return None
        return self._p123_api.create_folder(fileitem=fileitem, name=name)

    def download_file(
        self, fileitem: schemas.FileItem, path: Path = None
    ) -> Optional[Path]:
        if fileitem.storage != self._disk_name:
            return None
        return self._p123_api.download(fileitem, path)

    def upload_file(
        self, fileitem: schemas.FileItem, path: Path, new_name: Optional[str] = None
    ) -> Optional[schemas.FileItem]:
        if fileitem.storage != self._disk_name:
            return None
        return self._p123_api.upload(fileitem, path, new_name)

    def delete_file(self, fileitem: schemas.FileItem) -> Optional[bool]:
        if fileitem.storage != self._disk_name:
            return None
        return self._p123_api.delete(fileitem)

    def rename_file(self, fileitem: schemas.FileItem, name: str) -> Optional[bool]:
        if fileitem.storage != self._disk_name:
            return None
        return self._p123_api.rename(fileitem, name)

    def exists(self, fileitem: schemas.FileItem) -> Optional[bool]:
        if fileitem.storage != self._disk_name:
            return None
        return True if self.get_item(fileitem) else False

    def get_item(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        if fileitem.storage != self._disk_name:
            return None
        return self.get_file_item(storage=fileitem.storage, path=Path(fileitem.path))

    def get_file_item(self, storage: str, path: Path) -> Optional[schemas.FileItem]:
        if storage != self._disk_name:
            return None
        return self._p123_api.get_item(path)

    def get_parent_item(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        if fileitem.storage != self._disk_name:
            return None
        return self._p123_api.get_parent(fileitem)

    def snapshot_storage(
        self,
        storage: str,
        path: Path,
        last_snapshot_time: float = None,
        max_depth: int = 5,
    ) -> Optional[Dict[str, Dict]]:
        if storage != self._disk_name:
            return None

        files_info = {}

        def __snapshot_file(_fileitm: schemas.FileItem, current_depth: int = 0):
            try:
                if _fileitm.type == "dir":
                    if current_depth >= max_depth:
                        return
                    if (
                        self.snapshot_check_folder_modtime  # noqa
                        and last_snapshot_time
                        and _fileitm.modify_time
                        and _fileitm.modify_time <= last_snapshot_time
                    ):
                        return
                    sub_files = self._p123_api.list(_fileitm)
                    for sub_file in sub_files:
                        __snapshot_file(sub_file, current_depth + 1)
                else:
                    if getattr(_fileitm, "modify_time", 0) > last_snapshot_time:
                        files_info[_fileitm.path] = {
                            "size": _fileitm.size or 0,
                            "modify_time": getattr(_fileitm, "modify_time", 0),
                            "type": _fileitm.type,
                        }
            except Exception as e:
                logger.debug(f"Snapshot error for {_fileitm.path}: {e}")

        fileitem = self._p123_api.get_item(path)
        if not fileitem:
            return {}

        __snapshot_file(fileitem)
        return files_info

    def storage_usage(self, storage: str) -> Optional[schemas.StorageUsage]:
        if storage != self._disk_name:
            return None
        return self._p123_api.usage()

    def support_transtype(self, storage: str) -> Optional[dict]:
        if storage != self._disk_name:
            return None
        return {"move": "移动", "copy": "复制"}

    def stop_service(self):
        pass