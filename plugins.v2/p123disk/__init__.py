from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional
from .tool import P123AutoClient
from .p123_api import P123Api
from app import schemas
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType
from app.helper.storage import StorageHelper
from app.schemas import StorageOperSelectionEventData, FileItem

class P123Disk(_PluginBase):
    plugin_name = "123云盘储存(自用版)"
    plugin_desc = "基于123云盘储存二开(DDSRem-Dev)"
    plugin_icon = "https://raw.githubusercontent.com/juzi8633/MoviePilot-Plugins/main/icons/P123Disk.png"
    plugin_version = "1.8.0"
    plugin_author = "juzi8633"
    plugin_config_prefix = "p123disk_"
    plugin_order = 99
    auth_level = 1

    _enabled = False
    _client = None
    _disk_name = "123云盘"
    _p123_api = None
    
    # 忽略文件列表 (临时文件/元数据)
    _IGNORED_SUFFIXES = (
        '.!qB', '.part', '.aria2', '.tmp', 
        '.downloading', '.crdownload', '.ds_store'
    )

    def init_plugin(self, config: dict = None):
        if not config: return
        
        # 1. 注册存储
        storage_helper = StorageHelper()
        if not any(s.type == self._disk_name for s in storage_helper.get_storagies()):
            storage_helper.add_storage(storage=self._disk_name, name=self._disk_name, conf={})

        self._enabled = config.get("enabled")
        
        try:
            # 2. 初始化客户端 (仅在启用时)
            if self._enabled:
                self._client = P123AutoClient(config.get("passport"), config.get("password"))
                self._p123_api = P123Api(
                    client=self._client,
                    disk_name=self._disk_name,
                    webhook_url=config.get("webhook_url"),
                    webhook_secret=config.get("webhook_secret"),
                    upload_threads=config.get("upload_threads")
                )
                logger.info(f"【123】插件加载成功，当前版本: {self.plugin_version}")
        except Exception as e:
            logger.error(f"【123】插件初始化失败: {e}")

    def get_state(self) -> bool: return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow", 
                        "content": [{
                            "component": "VCol", 
                            "props": {"cols": 12}, 
                            "content": [{
                                "component": "VSwitch", 
                                "props": {"model": "enabled", "label": "启用插件"}
                            }]
                        }]
                    },
                    {
                        "component": "VRow", 
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "passport", "label": "手机号"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "password", "label": "密码"}}]}
                        ]
                    },
                    {
                        "component": "VRow", 
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "webhook_url", "label": "Webhook URL (可选)"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "webhook_secret", "label": "Webhook Secret (可选)"}}]}
                        ]
                    },
                    {
                        "component": "VRow", 
                        "content": [{
                            "component": "VCol", 
                            "props": {"cols": 12}, 
                            "content": [{
                                "component": "VTextField", 
                                "props": {"model": "upload_threads", "label": "上传线程数 (默认8)", "placeholder": "1-32"}
                            }]
                        }]
                    }
                ]
            }
        ], {
            "enabled": False, "passport": "", "password": "", 
            "webhook_url": "", "webhook_secret": "", "upload_threads": "8"
        }

    def get_module(self) -> Dict[str, Any]:
        return {
            "list_files": self.list_files, 
            "any_files": self.any_files, # 此方法已修复
            "upload_file": self.upload_file,
            "download_file": self.download_file, 
            "delete_file": self.delete_file,
            "rename_file": self.rename_file, 
            "get_file_item": self.get_file_item,
            "storage_usage": self.storage_usage, 
            "exists": self.exists,
            "create_folder": self.create_folder, 
            "get_parent_item": self.get_parent_item,
            "snapshot_storage": self.snapshot_storage, 
            "support_transtype": self.support_transtype,
            "get_item": self.get_item
        }

    @eventmanager.register(ChainEventType.StorageOperSelection)
    def storage_oper_selection(self, event: Event):
        if self._enabled and event.event_data.storage == self._disk_name:
            event.event_data.storage_oper = self._p123_api

    # --- 代理方法实现 ---

    def list_files(self, fileitem: schemas.FileItem, recursion: bool = False):
        if fileitem.storage != self._disk_name: return None
        # 递归处理
        result = []
        def _scan(item, rec):
            sub = self._p123_api.list(item)
            if not sub: return
            if rec:
                for s in sub:
                    if s.type == "dir": _scan(s, rec)
                    else: result.append(s)
            else: result.extend(sub)
        _scan(fileitem, recursion)
        return result

    def any_files(self, fileitem: schemas.FileItem, extensions: list = None) -> Optional[bool]:
        """
        判断是否存在指定后缀的文件 (v1.7.5 遗漏修复)
        """
        if fileitem.storage != self._disk_name: return None
        
        def __any_file(_item: FileItem):
            _items = self._p123_api.list(_item)
            if _items:
                if not extensions: return True
                for t in _items:
                    if t.type == "file" and t.extension and f".{t.extension.lower()}" in extensions:
                        return True
                    elif t.type == "dir":
                        if __any_file(t): return True
            return False
            
        return __any_file(fileitem)

    def upload_file(self, fileitem: schemas.FileItem, path: Path, new_name: Optional[str] = None):
        """
        上传文件 (带过滤逻辑)
        """
        if fileitem.storage != self._disk_name: return None
        
        # 临时文件/隐藏文件过滤
        fname = (new_name or path.name).lower()
        if any(fname.endswith(s) for s in self._IGNORED_SUFFIXES) or fname.startswith('.'):
            logger.debug(f"【123】忽略文件: {fname}")
            return None
            
        return self._p123_api.upload(fileitem, path, new_name)

    def snapshot_storage(self, storage: str, path: Path, last_snapshot_time: float = None, max_depth: int = 5):
        """
        生成存储快照
        """
        if storage != self._disk_name: return None
        files_info = {}

        def __snapshot_file(_fileitm: schemas.FileItem, current_depth: int = 0):
            try:
                if _fileitm.type == "dir":
                    if current_depth >= max_depth: return
                    # 增量检查
                    if (
                        getattr(self, "snapshot_check_folder_modtime", False)
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
        if not fileitem: return {}
        
        __snapshot_file(fileitem)
        return files_info

    # 简单代理方法 (明确展开，防止误解)
    def download_file(self, fileitem: schemas.FileItem, path: Path = None):
        if fileitem.storage != self._disk_name: return None
        return self._p123_api.download(fileitem, path)

    def delete_file(self, fileitem: schemas.FileItem):
        if fileitem.storage != self._disk_name: return None
        return self._p123_api.delete(fileitem)

    def rename_file(self, fileitem: schemas.FileItem, name: str):
        if fileitem.storage != self._disk_name: return None
        return self._p123_api.rename(fileitem, name)

    def create_folder(self, fileitem: schemas.FileItem, name: str):
        if fileitem.storage != self._disk_name: return None
        return self._p123_api.create_folder(fileitem, name)

    def exists(self, fileitem: schemas.FileItem):
        if fileitem.storage != self._disk_name: return None
        return True if self._p123_api.get_item(Path(fileitem.path)) else False

    def get_file_item(self, storage: str, path: Path):
        if storage != self._disk_name: return None
        return self._p123_api.get_item(path)
        
    def get_item(self, fileitem: schemas.FileItem):
        if fileitem.storage != self._disk_name: return None
        return self._p123_api.get_item(Path(fileitem.path))

    def get_parent_item(self, fileitem: schemas.FileItem):
        if fileitem.storage != self._disk_name: return None
        return self._p123_api.get_parent(fileitem)

    def storage_usage(self, storage: str):
        if storage != self._disk_name: return None
        return self._p123_api.usage()

    def support_transtype(self, storage: str):
        if storage != self._disk_name: return None
        return {"move": "移动", "copy": "复制"}
    
    def stop_service(self):
        pass
