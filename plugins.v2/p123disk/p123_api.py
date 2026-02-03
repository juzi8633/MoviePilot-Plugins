import ast
import time
import threading
import copy
import mmap
import json
import traceback
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Dict
from hashlib import md5
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, ReadTimeout, ConnectTimeout

from p123client import check_response
from app import schemas
from app.log import logger
from app.core.config import settings, global_vars
from app.modules.filemanager.storages import transfer_process
from app.utils.string import StringUtils
from .tool import P123AutoClient

# --- 内置零依赖 TTL 缓存 ---
class SimpleTTLCache:
    """
    一个简单的基于内存的 LRU + TTL 缓存实现，
    替代 cachetools 以避免依赖问题。
    """
    def __init__(self, maxsize=5000, ttl=3600):
        self.cache = OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl

    def __setitem__(self, key, value):
        # 如果键已存在，先删除以更新位置
        if key in self.cache:
            del self.cache[key]
        self.cache[key] = (value, time.time())
        # 超出容量清除最早的
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)

    def __getitem__(self, key):
        if key not in self.cache:
            raise KeyError(key)
        value, timestamp = self.cache[key]
        # 检查过期
        if time.time() - timestamp > self.ttl:
            del self.cache[key]
            raise KeyError(key)
        self.cache.move_to_end(key) # 标记为最近使用
        return value
    
    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __contains__(self, key):
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    def __delitem__(self, key):
        if key in self.cache:
            del self.cache[key]

class P123Api:
    """
    123云盘基础操作 (优化版 v1.8.0)
    集成: 多线程上传, 连接池, 零依赖缓存, mmap, Webhook
    """
    # 是否开启详细调试日志 (避免生产环境日志爆炸)
    DEBUG_MODE = False
    
    # 初始化缓存和 Webhook 线程池
    _id_cache = SimpleTTLCache(maxsize=10000, ttl=3600)
    _webhook_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="P123Webhook")

    def __init__(self, client: P123AutoClient, disk_name: str, 
                 webhook_url: str = None, 
                 webhook_secret: str = None, 
                 upload_threads: int = 8):
        self.client = client
        self._disk_name = disk_name
        self.webhook_url = webhook_url
        self.webhook_secret = webhook_secret
        self.transtype = {"move": "移动", "copy": "复制"}
        
        # 线程数限制处理
        try:
            self.max_upload_threads = int(upload_threads) if upload_threads else 8
            if self.max_upload_threads < 1: self.max_upload_threads = 1
        except:
            self.max_upload_threads = 8

        # 初始化全局连接池
        self._session = requests.Session()
        pool_size = max(self.max_upload_threads, 16)
        adapter = HTTPAdapter(
            pool_connections=pool_size, 
            pool_maxsize=pool_size * 2, 
            max_retries=2
        )
        self._session.mount('https://', adapter)
        self._session.mount('http://', adapter)
        
        logger.debug(f"【123】API实例初始化 | 并发: {self.max_upload_threads} | Webhook: {bool(webhook_url)}")

    def _path_to_id(self, path: str):
        """
        将路径转换为 FileID，带缓存和重试机制
        """
        if path == "/": return "0"
        if len(path) > 1 and path.endswith("/"): path = path[:-1]
        
        # 查缓存
        if path in self._id_cache: return self._id_cache[path]
            
        current_id = 0
        parent_path = "/"
        
        # 尝试利用缓存中的最近父节点加速查找
        for p in Path(path).parents:
            if str(p) in self._id_cache:
                parent_path = str(p)
                current_id = self._id_cache[parent_path]
                break
        
        try:
            rel_path = Path(path).relative_to(parent_path)
            # 如果是当前目录，直接返回ID
            if str(rel_path) == ".":
                 return str(current_id)

            for part in Path(rel_path).parts:
                find_part = False
                page = 1
                _next = 0
                first_find = True
                
                # 逐级查找
                while True:
                    payload = {
                        "limit": 100, "next": _next, "Page": page, 
                        "parentFileId": int(current_id), "inDirectSpace": "false"
                    }
                    if not first_find: time.sleep(0.5)
                    first_find = False
                    
                    # 列表请求重试逻辑
                    resp = None
                    for _ in range(3):
                        try:
                            resp = self.client.fs_list(payload)
                            check_response(resp)
                            break
                        except (ReadTimeout, ConnectTimeout):
                            time.sleep(1)
                        except Exception as e:
                            # 严重错误直接抛出
                            raise e
                    
                    if not resp:
                        raise Exception(f"列出目录超时: {current_id}")

                    item_list = resp.get("data").get("InfoList")
                    if not item_list: break
                    
                    for item in item_list:
                        if item["FileName"] == part:
                            current_id = item["FileId"]
                            find_part = True
                            break
                    if find_part or resp.get("data").get("Next") == "-1": break
                    page += 1
                    _next = resp.get("data").get("Next")
                        
                if not find_part:
                    # 路径不存在，清理缓存并报错
                    if path in self._id_cache: del self._id_cache[path]
                    raise FileNotFoundError(f"【123】路径不存在: {path}")
            
            self._id_cache[path] = str(current_id)
            return str(current_id)
        except Exception as e:
            if path in self._id_cache: del self._id_cache[path]
            raise e

    def list(self, fileitem: schemas.FileItem) -> List[schemas.FileItem]:
        """
        列出文件
        """
        if fileitem.type == "file":
            item = self.detail(fileitem)
            return [item] if item else []
        
        file_id = "0" if fileitem.path == "/" else (fileitem.fileid or self._path_to_id(fileitem.path))
        items = []
        try:
            page = 1
            _next = 0
            while True:
                payload = {
                    "limit": 100, "next": _next, "Page": page, 
                    "parentFileId": int(file_id), "inDirectSpace": "false"
                }
                resp = self.client.fs_list(payload)
                check_response(resp)
                
                item_list = resp.get("data").get("InfoList")
                if not item_list: break
                
                for item in item_list:
                    path = f"{fileitem.path}{item['FileName']}"
                    self._id_cache[path] = str(item["FileId"])
                    items.append(self._parse_item(item, fileitem.path))
                    
                if resp.get("data").get("Next") == "-1": break
                page += 1
                _next = resp.get("data").get("Next")
        except Exception as e:
            logger.debug(f"【123】List error: {e}")
            return items
        return items

    def _parse_item(self, item, parent_path):
        """统一解析API返回的Item为FileItem"""
        path = f"{parent_path}{item['FileName']}"
        file_path = path + ("/" if item["Type"] == 1 else "")
        return schemas.FileItem(
            storage=self._disk_name,
            fileid=str(item["FileId"]),
            parent_fileid=str(item.get("ParentFileId", "")),
            name=item["FileName"],
            basename=Path(item["FileName"]).stem,
            extension=Path(item["FileName"]).suffix[1:] if item["Type"] == 0 else None,
            type="dir" if item["Type"] == 1 else "file",
            path=file_path,
            size=item.get("Size") if item["Type"] == 0 else None,
            modify_time=int(datetime.fromisoformat(item["UpdateAt"]).timestamp()),
            pickcode=str(item),
        )

    def create_folder(self, fileitem: schemas.FileItem, name: str) -> Optional[schemas.FileItem]:
        try:
            parent_id = self._path_to_id(fileitem.path)
            resp = self.client.fs_mkdir(name, parent_id=parent_id)
            check_response(resp)
            data = resp["data"]["Info"]
            new_path = Path(fileitem.path) / name
            self._id_cache[str(new_path)] = str(data["FileId"])
            
            # 手动构造返回，因为mkdir返回的数据结构可能不完整
            return schemas.FileItem(
                storage=self._disk_name,
                fileid=str(data["FileId"]),
                path=str(new_path) + "/",
                name=name,
                basename=name,
                type="dir",
                modify_time=int(datetime.fromisoformat(data["UpdateAt"]).timestamp()),
                pickcode=str(data),
            )
        except Exception as e:
            logger.error(f"【123】创建目录失败: {e}")
            return None

    def _upload_chunk_worker(self, session, data_source, slice_no, offset, size, upload_data, target_name, file_lock=None):
        """
        分片上传的工作线程
        """
        chunk = None
        try:
            # 读取数据：支持 mmap 或 文件锁读取
            if isinstance(data_source, mmap.mmap):
                chunk = data_source[offset : offset + size]
            elif file_lock:
                with file_lock:
                    data_source.seek(offset)
                    chunk = data_source.read(size)
            else:
                return 0
        except Exception:
            return 0
        
        if not chunk: return 0
        
        current_data = copy.deepcopy(upload_data)
        current_data["partNumberStart"] = slice_no
        current_data["partNumberEnd"] = slice_no + 1
        
        # 重试逻辑 (指数退避)
        for i in range(5):
            try:
                # 1. 获取上传 URL
                url_resp = self.client.upload_prepare(current_data)
                check_response(url_resp)
                upload_url = url_resp["data"]["presignedUrls"][str(slice_no)]
                
                # 2. 执行 PUT 上传
                resp = session.put(upload_url, data=chunk, headers={"authorization": ""}, timeout=300)
                if resp.status_code == 200:
                    return len(chunk)
                else:
                    raise Exception(f"HTTP {resp.status_code}")
            except Exception as e:
                if i == 4: # 最后一次重试失败，抛出异常
                    logger.error(f"【123】分片 {slice_no} 上传失败: {e}")
                    raise e
                time.sleep(2 ** i) # 1s, 2s, 4s, 8s...
        return 0

    def upload(self, target_dir: schemas.FileItem, local_path: Path, new_name: Optional[str] = None) -> Optional[schemas.FileItem]:
        """
        核心上传逻辑
        """
        target_name = new_name or local_path.name
        target_path = Path(target_dir.path) / target_name
        file_size = local_path.stat().st_size
        
        logger.info(f"【123】开始上传: {target_name} ({StringUtils.str_filesize(file_size)})")

        # 1. 计算MD5
        file_md5 = ""
        try:
            with open(local_path, "rb") as f:
                hash_md5 = md5()
                # 使用 64MB buffer 读取
                for chunk in iter(lambda: f.read(64*1024*1024), b""):
                    hash_md5.update(chunk)
                file_md5 = hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"【123】MD5计算失败: {e}")
            return None

        # 2. 预检 / 秒传
        try:
            parent_id = target_dir.fileid or self._path_to_id(target_dir.path)
            resp = self.client.upload_request({
                "etag": file_md5, "fileName": target_name, "size": file_size,
                "parentFileId": int(parent_id), "type": 0, "duplicate": 2
            })
            check_response(resp)
            
            # 场景: 秒传命中
            if resp.get("data", {}).get("Reuse"):
                logger.info(f"【123】秒传成功: {target_name}")
                data = resp["data"]["Info"]
                self._trigger_webhook(data["FileName"], str(target_path), file_size, file_md5, "rapid_upload")
                return self._parse_item(data, target_dir.path)
                
            upload_data = resp["data"]
            slice_size = int(upload_data.get("SliceSize", 10*1024*1024))
            
            # 3. 执行上传 (分片 or 直传)
            if file_size > slice_size:
                # === 大文件多线程分片上传 ===
                logger.info(f"【123】并发上传 ({self.max_upload_threads}线程): {target_name}")
                progress_callback = transfer_process(local_path.as_posix())
                
                tasks = []
                offset = 0
                slice_no = 1
                while offset < file_size:
                    sz = min(slice_size, file_size - offset)
                    tasks.append((slice_no, offset, sz))
                    offset += sz
                    slice_no += 1
                
                f = open(local_path, "rb")
                mmapped_file = None
                file_lock = None
                use_mmap = True
                
                try:
                    # 尝试 mmap 内存映射，提升大文件读取性能
                    try:
                        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                    except:
                        # 失败降级为普通文件读取（带锁）
                        use_mmap = False
                        file_lock = threading.Lock()
                        f.seek(0)
                    
                    data_source = mmapped_file if use_mmap else f
                    downloaded = 0
                    
                    with ThreadPoolExecutor(max_workers=self.max_upload_threads) as executor:
                        futures = {
                            executor.submit(
                                self._upload_chunk_worker, 
                                self._session, 
                                data_source, 
                                sno, off, sz, 
                                upload_data, 
                                target_name, 
                                file_lock
                            ): sz for sno, off, sz in tasks
                        }
                        
                        for future in as_completed(futures):
                            if global_vars.is_transfer_stopped(local_path.as_posix()):
                                executor.shutdown(wait=False)
                                raise Exception("上传被人为取消")
                            
                            downloaded += future.result()
                            if file_size: 
                                progress_callback((downloaded * 100) / file_size)
                            
                finally:
                    if mmapped_file: mmapped_file.close()
                    f.close()
                progress_callback(100)
                
            else:
                # === 小文件直传 ===
                logger.info(f"【123】直传文件: {target_name}")
                auth_resp = self.client.upload_auth(upload_data)
                check_response(auth_resp)
                url = auth_resp["data"]["presignedUrls"]["1"]
                
                # 直传重试
                for _ in range(3):
                    try:
                        with open(local_path, "rb") as f:
                            self._session.put(url, data=f, timeout=300)
                        break
                    except Exception:
                        time.sleep(1)
            
            # 4. 完成上传确认
            upload_data["isMultipart"] = file_size > slice_size
            comp_resp = self.client.upload_complete(upload_data)
            check_response(comp_resp)
            
            # 结果处理
            data = comp_resp.get("data", {}).get("file_info", {})
            # 兼容处理：部分情况下 code=0 但 data 为空（通常是大文件后台合并中）
            if not data and comp_resp.get("code") == 0:
                data = {
                    "FileId": "", 
                    "FileName": target_name, 
                    "Type": 0, 
                    "Size": file_size, 
                    "UpdateAt": datetime.now().isoformat()
                }

            self._trigger_webhook(data.get("FileName", target_name), str(target_path), file_size, file_md5, "uploaded")
            return self._parse_item(data, target_dir.path)

        except Exception as e:
            logger.error(f"【123】上传失败 {target_name}: {e}")
            if self.DEBUG_MODE: logger.error(traceback.format_exc())
            return None

    def _trigger_webhook(self, name, path, size, etag, status):
        """异步发送Webhook通知"""
        if not self.webhook_url: return
        self._webhook_executor.submit(self._do_webhook, name, path, size, etag, status)
        
    def _do_webhook(self, name, path, size, etag, status):
        try:
            self._session.post(self.webhook_url, json={
                "file_name": name, 
                "path": path, 
                "size": size, 
                "etag": etag,
                "secret": self.webhook_secret, 
                "status": status, 
                "timestamp": int(time.time())
            }, timeout=5)
        except Exception as e: 
            if self.DEBUG_MODE: logger.warning(f"Webhook发送失败: {e}")

    # --- 基础文件操作代理 ---
    
    def delete(self, fileitem: schemas.FileItem) -> bool:
        try:
            self.client.fs_trash(int(fileitem.fileid), event="intoRecycle")
            if fileitem.path in self._id_cache: del self._id_cache[fileitem.path]
            return True
        except: return False

    def rename(self, fileitem: schemas.FileItem, name: str) -> bool:
        try:
            self.client.fs_rename({"FileId": int(fileitem.fileid), "fileName": name, "duplicate": 2})
            if fileitem.path in self._id_cache: del self._id_cache[fileitem.path]
            return True
        except: return False
        
    def usage(self) -> Optional[schemas.StorageUsage]:
        try:
            resp = self.client.user_info()
            return schemas.StorageUsage(
                total=resp["data"]["SpacePermanent"],
                available=int(resp["data"]["SpacePermanent"]) - int(resp["data"]["SpaceUsed"]),
            )
        except: return None
        
    def download(self, fileitem: schemas.FileItem, path: Path = None) -> Optional[Path]:
        local_path = None
        try:
            # 获取下载链接
            json_obj = ast.literal_eval(fileitem.pickcode)
            resp = self.client.download_info({
                "Etag": json_obj["Etag"], "FileID": int(fileitem.fileid), "FileName": fileitem.name,
                "S3KeyFlag": json_obj["S3KeyFlag"], "Size": int(json_obj["Size"])
            })
            check_response(resp)
            url = resp["data"]["DownloadUrl"]
            
            local_path = path or settings.TEMP_PATH / fileitem.name
            logger.info(f"【123】开始下载: {fileitem.name}")
            
            progress = transfer_process(fileitem.path)
            # 使用Session复用连接下载
            with self._session.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    dl_size = 0
                    for chunk in r.iter_content(10*1024*1024):
                        if global_vars.is_transfer_stopped(fileitem.path): 
                            logger.info("【123】下载取消")
                            return None
                        if chunk:
                            f.write(chunk)
                            dl_size += len(chunk)
                            if fileitem.size: progress(dl_size * 100 / fileitem.size)
            progress(100)
            return local_path
        except Exception as e:
            logger.error(f"【123】下载失败: {e}")
            if local_path and local_path.exists(): local_path.unlink()
            return None

    # Getters
    def get_item(self, path: Path):
        try:
            fid = self._path_to_id(str(path))
            resp = self.client.fs_info(int(fid))
            return self._parse_item(resp["data"]["infoList"][0], str(path.parent) + "/")
        except: return None

    def get_parent(self, item): return self.get_item(Path(item.path).parent)
    def detail(self, item): return self.get_item(Path(item.path))
    def support_transtype(self): return self.transtype
    # 增加 get_folder 以兼容部分调用
    def get_folder(self, path: Path): return self.get_item(path)
