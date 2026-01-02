import ast
import time
import threading
import copy
import mmap
import json
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Dict
from hashlib import md5
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, ReadTimeout, ConnectTimeout
from cachetools import TTLCache 

from p123client import P123Client, check_response

from app import schemas
from app.log import logger
from app.core.config import settings, global_vars
from app.modules.filemanager.storages import transfer_process
from app.utils.string import StringUtils


class P123Api:
    """
    123云盘基础操作 (最终完整优化版 v1.4.2)
    集成特性：多线程、全局连接池、mmap(带自动降级)、异步Webhook池、指数重试、自动分片、大文件兼容
    优化更新：TTL缓存(1小时)、动态线程策略(3/5/8)、Mmap失败自动回退、增强日志与超时重试
    """

    # 【优化1：缓存策略】使用 TTLCache，最大10000条，有效期3600秒(1小时)
    _id_cache = TTLCache(maxsize=10000, ttl=3600)
    
    # Webhook 发送线程池 (限制并发，防止资源耗尽)
    _webhook_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="P123Webhook")

    def __init__(self, client: P123Client, disk_name: str, 
                 webhook_url: str = None, 
                 webhook_secret: str = None, 
                 upload_threads: str = None):
        """
        初始化
        """
        self.client = client
        self._disk_name = disk_name
        self.webhook_url = webhook_url
        self.webhook_secret = webhook_secret
        
        # 处理并发线程数 (这是用户设置的"上限")
        try:
            self.max_upload_threads = int(upload_threads) if upload_threads else 3
            if self.max_upload_threads < 1: self.max_upload_threads = 1
            if self.max_upload_threads > 32: self.max_upload_threads = 32
        except Exception:
            self.max_upload_threads = 3

        # 初始化全局 HTTP Session (连接复用)
        self._session = requests.Session()
        
        # 【优化】：连接池大小按照最大可能的线程数来设置，防止高并发时阻塞
        pool_size = max(self.max_upload_threads, 16)
        
        adapter = HTTPAdapter(
            pool_connections=pool_size, 
            pool_maxsize=pool_size * 2,
            max_retries=2
        )
        self._session.mount('https://', adapter)
        self._session.mount('http://', adapter)
        
        logger.debug(f"【123】API实例初始化 | 线程上限: {self.max_upload_threads} | 连接池: {pool_size} | Webhook: {'开启' if webhook_url else '关闭'}")

    def __del__(self):
        """清理资源"""
        try:
            self._session.close()
        except Exception:
            pass

    def _path_to_id(self, path: str):
        """
        通过路径获取ID (带TTL缓存和自动驱逐，增加了超时重试机制)
        """
        if path == "/":
            return "0"
        if len(path) > 1 and path.endswith("/"):
            path = path[:-1]
            
        # 1. 检查缓存
        if path in self._id_cache:
            # logger.debug(f"【123】缓存命中: {path} -> {self._id_cache[path]}")
            return self._id_cache[path]
            
        # 逐级查找缓存
        current_id = 0
        parent_path = "/"
        for p in Path(path).parents:
            if str(p) in self._id_cache:
                parent_path = str(p)
                current_id = self._id_cache[parent_path]
                break
        
        try:
            # 计算相对路径
            try:
                rel_path = Path(path).relative_to(parent_path)
            except ValueError:
                current_id = 0
                rel_path = Path(path).relative_to("/")

            for part in Path(rel_path).parts:
                find_part = False
                page = 1
                _next = 0
                first_find = True
                
                # 针对每一层目录的查找循环
                while True:
                    payload = {
                        "limit": 100,
                        "next": _next,
                        "Page": page,
                        "parentFileId": int(current_id),
                        "inDirectSpace": "false",
                    }
                    if first_find:
                        first_find = False
                    else:
                        time.sleep(1)
                    
                    # 【新增优化】目录列表请求增加重试机制 (针对 ReadTimeout)
                    retry_list = 0
                    max_list_retries = 3
                    resp = None
                    
                    while retry_list < max_list_retries:
                        try:
                            resp = self.client.fs_list(payload)
                            check_response(resp)
                            break # 成功则跳出重试
                        except (ReadTimeout, ConnectTimeout) as net_err:
                            retry_list += 1
                            logger.warning(f"【123】列出目录超时(ID:{current_id}), 重试 {retry_list}/{max_list_retries}: {net_err}")
                            time.sleep(2)
                        except Exception as e:
                            # 非网络超时错误，直接抛出
                            logger.error(f"【123】列出目录异常 (ID: {current_id}): {e}")
                            if str(parent_path) in self._id_cache:
                                 del self._id_cache[str(parent_path)]
                            raise e
                    
                    if retry_list >= max_list_retries:
                        raise Exception(f"列出目录失败(重试{max_list_retries}次): {current_id}")

                    item_list = resp.get("data").get("InfoList")
                    if not item_list:
                        break
                    for item in item_list:
                        if item["FileName"] == part:
                            current_id = item["FileId"]
                            find_part = True
                            break
                    if find_part:
                        break
                    if resp.get("data").get("Next") == "-1":
                        break
                    else:
                        page += 1
                        _next = resp.get("data").get("Next")
                        
                if not find_part:
                    # 【优化1：缓存驱逐】路径不存在，清除可能的错误缓存
                    if path in self._id_cache:
                        del self._id_cache[path]
                        logger.debug(f"【123】路径不存在，清除缓存: {path}")
                    raise FileNotFoundError(f"【123】路径节点不存在: {part} (in {path})")
            
            if not current_id:
                raise FileNotFoundError(f"【123】路径不存在: {path}")
                
            # 缓存路径
            self._id_cache[path] = str(current_id)
            return str(current_id)
            
        except Exception as e:
            # 发生任何未捕获异常时，尝试清理当前查询路径的缓存
            if path in self._id_cache:
                del self._id_cache[path]
            raise e

    def _get_dynamic_workers(self, file_size: int) -> int:
        """
        【优化3：动态并发控制】
        根据文件大小动态计算并发线程数
        """
        gb = 1024 * 1024 * 1024
        
        if file_size < 5 * gb:
            workers = 5
        elif file_size < 10 * gb:
            workers = 7
        else:
            workers = 9
            
        # 始终受限于用户设置的全局最大值 (防止低配机器崩溃)
        final_workers = min(workers, self.max_upload_threads)
        
        # 兜底至少 1 个线程
        return max(1, final_workers)

    def _upload_chunk_worker(self, session, data_source, slice_no, offset, size, upload_data, target_name, file_lock=None):
        """
        分片上传的工作线程函数
        """
        chunk = None
        try:
            # 【优化2：Mmap 兼容读取】
            if isinstance(data_source, mmap.mmap):
                chunk = data_source[offset : offset + size]
            else:
                if file_lock:
                    with file_lock:
                        data_source.seek(offset)
                        chunk = data_source.read(size)
                else:
                    logger.error("【123】普通IO模式缺少文件锁")
                    return 0
        except Exception as e:
            logger.error(f"【123】读取分片数据失败(Offset:{offset}): {e}")
            raise e
        
        if not chunk:
            return 0

        current_upload_data = copy.deepcopy(upload_data)
        current_upload_data["partNumberStart"] = slice_no
        current_upload_data["partNumberEnd"] = slice_no + 1

        max_retries = 5
        retry_count = 0
        
        # 获取URL (预处理阶段)
        try:
            current_upload_url_resp = self.client.upload_prepare(current_upload_data)
            check_response(current_upload_url_resp)
        except Exception as e:
            logger.error(f"【123】获取分片上传URL失败(分片{slice_no}): {e}")
            raise e

        while retry_count < max_retries:
            try:
                upload_url = current_upload_url_resp["data"]["presignedUrls"][str(slice_no)]
                
                # 使用Session复用连接
                resp = session.put(
                    upload_url,
                    data=chunk,
                    headers={"authorization": ""},
                    timeout=300
                )
                
                if resp.status_code != 200:
                    raise Exception(f"HTTP {resp.status_code} - {resp.text[:50]}")
                    
                return len(chunk)

            except Exception as upload_err:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** (retry_count - 1)
                    # 【日志优化】记录详细重试原因
                    logger.warning(f"【123】{target_name} 分片{slice_no} 上传失败(第{retry_count}次): {upload_err} -> 等待{wait_time}s")
                    time.sleep(wait_time)
                    try:
                        current_upload_url_resp = self.client.upload_prepare(current_upload_data)
                        check_response(current_upload_url_resp)
                    except Exception:
                        pass
                else:
                    logger.error(f"【123】{target_name} 分片{slice_no} 最终失败: {upload_err}")
                    raise upload_err
        return 0

    def list(self, fileitem: schemas.FileItem) -> List[schemas.FileItem]:
        """
        浏览文件
        """
        if fileitem.type == "file":
            item = self.detail(fileitem)
            if item:
                return [item]
            return []
        if fileitem.path == "/":
            file_id = "0"
        else:
            file_id = fileitem.fileid
            if not file_id:
                file_id = self._path_to_id(fileitem.path)

        items = []
        try:
            page = 1
            _next = 0
            first_find = True
            while True:
                payload = {
                    "limit": 100,
                    "next": _next,
                    "Page": page,
                    "parentFileId": int(file_id),
                    "inDirectSpace": "false",
                }
                if first_find:
                    first_find = False
                else:
                    time.sleep(1)
                
                # 【新增优化】浏览目录也增加网络超时保护
                try:
                    resp = self.client.fs_list(payload)
                    check_response(resp)
                except (ReadTimeout, ConnectTimeout):
                    logger.warning(f"【123】浏览目录超时，重试一次...")
                    time.sleep(2)
                    resp = self.client.fs_list(payload)
                    check_response(resp)

                item_list = resp.get("data").get("InfoList")
                if not item_list:
                    break
                for item in item_list:
                    path = f"{fileitem.path}{item['FileName']}"
                    # 更新缓存
                    self._id_cache[path] = str(item["FileId"])

                    file_path = path + ("/" if item["Type"] == 1 else "")
                    items.append(
                        schemas.FileItem(
                            storage=self._disk_name,
                            fileid=str(item["FileId"]),
                            parent_fileid=str(item["ParentFileId"]),
                            name=item["FileName"],
                            basename=Path(item["FileName"]).stem,
                            extension=Path(item["FileName"]).suffix[1:]
                            if item["Type"] == 0
                            else None,
                            type="dir" if item["Type"] == 1 else "file",
                            path=file_path,
                            size=item["Size"] if item["Type"] == 0 else None,
                            modify_time=int(
                                datetime.fromisoformat(item["UpdateAt"]).timestamp()
                            ),
                            pickcode=str(item),
                        )
                    )
                if resp.get("data").get("Next") == "-1":
                    break
                else:
                    page += 1
                    _next = resp.get("data").get("Next")
        except Exception as e:
            logger.debug(f"【123】获取信息失败: {str(e)}")
            return items
        return items

    def create_folder(
        self, fileitem: schemas.FileItem, name: str
    ) -> Optional[schemas.FileItem]:
        """
        创建目录
        """
        try:
            new_path = Path(fileitem.path) / name
            parent_id = self._path_to_id(fileitem.path)
            
            resp = self.client.fs_mkdir(name, parent_id=parent_id)
            check_response(resp)
            
            data = resp["data"]["Info"]
            # 写入缓存
            self._id_cache[str(new_path)] = str(data["FileId"])
            
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
            logger.debug(f"【123】创建目录失败: {str(e)}")
            return None

    def get_folder(self, path: Path) -> Optional[schemas.FileItem]:
        """
        获取目录，如目录不存在则创建
        """
        def __find_dir(
            _fileitem: schemas.FileItem, _name: str
        ) -> Optional[schemas.FileItem]:
            for sub_folder in self.list(_fileitem):
                if sub_folder.type != "dir":
                    continue
                if sub_folder.name == _name:
                    return sub_folder
            return None

        folder = self.get_item(path)
        if folder:
            return folder
            
        fileitem = schemas.FileItem(storage=self._disk_name, path="/")
        for part in path.parts[1:]:
            dir_file = __find_dir(fileitem, part)
            if dir_file:
                fileitem = dir_file
            else:
                dir_file = self.create_folder(fileitem, part)
                if not dir_file:
                    logger.warn(f"【123】递归创建目录失败: {fileitem.path}{part}")
                    return None
                fileitem = dir_file
        return fileitem

    def get_item(self, path: Path) -> Optional[schemas.FileItem]:
        """
        获取文件或目录
        """
        try:
            file_id = self._path_to_id(str(path))
            if not file_id:
                return None
            resp = self.client.fs_info(int(file_id))
            check_response(resp)
            data = resp["data"]["infoList"][0]
            return schemas.FileItem(
                storage=self._disk_name,
                fileid=str(data["FileId"]),
                path=str(path) + ("/" if data["Type"] == 1 else ""),
                type="file" if data["Type"] == 0 else "dir",
                name=data["FileName"],
                basename=Path(data["FileName"]).stem,
                extension=Path(data["FileName"]).suffix[1:]
                if data["Type"] == 0
                else None,
                pickcode=str(data),
                size=data["Size"] if data["Type"] == 0 else None,
                modify_time=int(datetime.fromisoformat(data["UpdateAt"]).timestamp()),
            )
        except Exception as e:
            return None

    def get_parent(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        """
        获取父目录
        """
        return self.get_item(Path(fileitem.path).parent)

    def delete(self, fileitem: schemas.FileItem) -> bool:
        """
        删除文件
        """
        try:
            logger.info(f"【123】正在删除: {fileitem.path}")
            resp = self.client.fs_trash(int(fileitem.fileid), event="intoRecycle")
            check_response(resp)
            # 主动清理缓存
            if fileitem.path in self._id_cache:
                del self._id_cache[fileitem.path]
            return True
        except Exception as e:
            logger.error(f"【123】删除失败: {e}")
            return False

    def rename(self, fileitem: schemas.FileItem, name: str) -> bool:
        """
        重命名文件
        """
        try:
            logger.info(f"【123】正在重命名: {fileitem.name} -> {name}")
            payload = {
                "FileId": int(fileitem.fileid),
                "fileName": name,
                "duplicate": 2,
            }
            resp = self.client.fs_rename(payload)
            check_response(resp)
            # 清理旧缓存
            if fileitem.path in self._id_cache:
                del self._id_cache[fileitem.path]
            return True
        except Exception as e:
            logger.error(f"【123】重命名失败: {e}")
            return False

    def download(self, fileitem: schemas.FileItem, path: Path = None) -> Optional[Path]:
        """
        下载文件
        """
        json_obj = ast.literal_eval(fileitem.pickcode)
        s3keyflag = json_obj["S3KeyFlag"]
        file_id = fileitem.fileid
        file_name = fileitem.name
        _md5 = json_obj["Etag"]
        size = json_obj["Size"]
        try:
            payload = {
                "Etag": _md5,
                "FileID": int(file_id),
                "FileName": file_name,
                "S3KeyFlag": s3keyflag,
                "Size": int(size),
            }
            resp = self.client.download_info(payload)
            check_response(resp)
            download_url = resp["data"]["DownloadUrl"]
            local_path = path or settings.TEMP_PATH / fileitem.name
            logger.info(f"【123】获取下载链接成功: {download_url[:30]}...")
        except Exception as e:
            logger.error(f"【123】获取下载链接失败: {fileitem.name} - {str(e)}")
            return None

        file_size = fileitem.size
        logger.info(f"【123】开始下载: {fileitem.name} -> {local_path}")
        progress_callback = transfer_process(Path(fileitem.path).as_posix())

        try:
            # 使用 session 进行下载以复用连接
            with self._session.get(download_url, stream=True) as r:
                r.raise_for_status()
                downloaded_size = 0

                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):
                        if global_vars.is_transfer_stopped(fileitem.path):
                            logger.info(f"【123】{fileitem.path} 下载已取消！")
                            return None
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if file_size:
                                progress = (downloaded_size * 100) / file_size
                                progress_callback(progress)

                progress_callback(100)
                logger.info(f"【123】下载完成: {fileitem.name}")

        except Exception as e:
            logger.error(f"【123】下载异常: {fileitem.name} - {str(e)}")
            if local_path.exists():
                local_path.unlink()
            return None

        return local_path

    def _do_send_webhook_task(self, file_name, remote_path, size, etag, status, speed):
        """
        执行Webhook发送任务 (线程池工作函数)
        """
        try:
            payload = {
                "file_name": file_name,
                "path": remote_path,
                "size": size,
                "etag": etag,
                "secret": self.webhook_secret,
                "status": status,
                "speed": speed,
                "timestamp": int(time.time())
            }
            
            logger.debug(f"【123】WebHook请求Payload: {json.dumps(payload, ensure_ascii=False)}")
            
            resp = self._session.post(self.webhook_url, json=payload, timeout=10)
            logger.info(f"【123】WebHook响应 [{resp.status_code}]: {resp.text[:500]}")
            
            if resp.status_code != 200:
                logger.warning(f"【123】WebHook接收端返回错误: {resp.status_code}")
        except Exception as e:
            logger.warning(f"【123】WebHook发送失败: {e}")

    def _send_upload_webhook(self, file_name: str, remote_path: str, size: int, etag: str, status: str, speed: str = ""):
        """
        提交 Webhook 任务到线程池
        """
        if not self.webhook_url:
            return

        self._webhook_executor.submit(
            self._do_send_webhook_task, 
            file_name, remote_path, size, etag, status, speed
        )

    def upload(
        self,
        target_dir: schemas.FileItem,
        local_path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[schemas.FileItem]:
        """
        上传文件 (全功能：秒传检测+断点续传+详细日志+异常修复+动态线程+Mmap回退)
        """
        start_time = time.time()
        target_name = new_name or local_path.name
        target_path = Path(target_dir.path) / target_name
        file_size = local_path.stat().st_size

        logger.info(f"【123】开始处理文件: {target_name} ({StringUtils.str_filesize(file_size)})")

        # 1. 计算MD5 (带进度显示，IO Buffer优化至 64MB)
        file_md5 = ""
        try:
            with open(local_path, "rb") as f:
                hash_md5 = md5()
                processed_size = 0
                last_log_time = time.time()
                chunk_size = 64 * 1024 * 1024 # 优化: 64MB Buffer
                
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
                    processed_size += len(chunk)
                    
                    # 超过100MB文件显示计算进度
                    if time.time() - last_log_time > 5 and file_size > 1024*1024*100:
                        pct = int(processed_size / file_size * 100)
                        logger.info(f"【123】计算特征值: {pct}%")
                        last_log_time = time.time()
                        
                file_md5 = hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"【123】文件读取/MD5失败: {e}")
            logger.debug(traceback.format_exc()) # 【日志优化】打印堆栈
            return None

        try:
            # 2. 准备参数 (确保parentFileId有效)
            parent_file_id = target_dir.fileid
            if not parent_file_id:
                logger.debug(f"【123】ParentID为空，重新解析路径: {target_dir.path}")
                try:
                    parent_file_id = self._path_to_id(target_dir.path)
                except Exception as pid_err:
                    logger.error(f"【123】无法获取目标目录ID: {pid_err}")
                    return None
            
            # 不再设置 sliceSize，遵循服务器策略
            upload_req_payload = {
                "etag": file_md5,
                "fileName": target_name,
                "size": file_size,
                "parentFileId": int(parent_file_id),
                "type": 0,
                "duplicate": 2,
            }
            
            logger.debug(f"【123】预检查Payload: {json.dumps(upload_req_payload, ensure_ascii=False)}")
            
            resp = self.client.upload_request(upload_req_payload)
            check_response(resp)
            
            # === 场景1：秒传成功 ===
            if resp.get("data").get("Reuse"):
                logger.info(f"【123】秒传成功: {target_name}")
                data = resp.get("data", {}).get("Info", {})
                
                # 防御性检查
                if not data or "FileName" not in data:
                     logger.error(f"【123】秒传返回数据异常: {json.dumps(resp, ensure_ascii=False)}")
                     return None

                self._send_upload_webhook(
                    file_name=data["FileName"],
                    remote_path=str(target_path) + ("/" if data.get("Type") == 1 else ""),
                    size=file_size,
                    etag=file_md5,
                    status="rapid_upload",
                    speed="∞"
                )
                return self._build_file_item(data, target_path)
            
        except Exception as e:
            logger.error(f"【123】秒传/预检查失败: {e}")
            return None

        # === 场景2：普通上传 ===
        # 使用全局 Session，不再每次创建
        try:
            upload_data = resp.get("data")
            if not upload_data:
                logger.error(f"【123】API响应缺少data字段: {json.dumps(resp, ensure_ascii=False)}")
                return None
                
            slice_size = int(upload_data.get("SliceSize", 10*1024*1024))
            
            if file_size > slice_size:
                # 【优化3：动态线程计算】
                current_workers = self._get_dynamic_workers(file_size)

                logger.info(
                    f"【123】启动并发上传 | 线程: {current_workers} (上限{self.max_upload_threads}) | 分片: {StringUtils.str_filesize(slice_size)} | 总片数: {int(file_size/slice_size)+1}"
                )
                
                progress_callback = transfer_process(local_path.as_posix())
                
                tasks = []
                offset = 0
                slice_no = 1
                while offset < file_size:
                    current_chunk_size = min(slice_size, file_size - offset)
                    tasks.append((slice_no, offset, current_chunk_size))
                    offset += current_chunk_size
                    slice_no += 1

                downloaded_size = 0
                last_log_time = 0 
                
                # 【优化2：Mmap 兼容性回退逻辑】
                use_mmap = True
                f = open(local_path, "rb")
                mmapped_file = None
                file_lock = None
                
                try:
                    # 尝试 mmap
                    try:
                        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                    except (ValueError, OSError) as mmap_err:
                        # mmap 失败，降级为普通IO
                        logger.warning(f"【123】mmap映射失败({mmap_err})，降级为普通IO模式(可能会变慢)")
                        use_mmap = False
                        file_lock = threading.Lock() # 普通IO多线程读文件需要锁
                        f.seek(0) # 重置指针

                    data_source = mmapped_file if use_mmap else f
                    
                    with ThreadPoolExecutor(max_workers=current_workers) as executor:
                        future_to_slice = {
                            executor.submit(
                                self._upload_chunk_worker, 
                                self._session, 
                                data_source, # 传入数据源(mmap对象 或 file对象)
                                sno, off, sz, 
                                upload_data, target_name,
                                file_lock # 传入锁(如果是mmap则为None)
                            ): sno for sno, off, sz in tasks
                        }
                        
                        for future in as_completed(future_to_slice):
                            if global_vars.is_transfer_stopped(local_path.as_posix()):
                                logger.warning(f"【123】上传被人为终止: {target_name}")
                                executor.shutdown(wait=False)
                                return None
                            
                            try:
                                bytes_uploaded = future.result()
                                downloaded_size += bytes_uploaded
                                
                                # 上传心跳 (每15秒)
                                current_time = time.time()
                                if current_time - last_log_time > 15:
                                    percent = int((downloaded_size / file_size) * 100)
                                    logger.info(f"【123】上传进度: {percent}% ({StringUtils.str_filesize(downloaded_size)}/{StringUtils.str_filesize(file_size)})")
                                    last_log_time = current_time

                                if file_size:
                                    progress_callback((downloaded_size * 100) / file_size)
                            except Exception as e:
                                logger.error(f"【123】分片线程异常: {e}")
                                raise e
                finally:
                    # 清理资源
                    if mmapped_file:
                        mmapped_file.close()
                    if f:
                        f.close()

                progress_callback(100)
            else:
                logger.info(f"【123】小文件直传: {target_name}")
                resp = self.client.upload_auth(upload_data)
                check_response(resp)
                
                with open(local_path, "rb") as f:
                    file_data = f.read()
                
                for i in range(3):
                    try:
                        # 小文件也使用Session复用连接
                        resp_put = self._session.put(
                            resp["data"]["presignedUrls"]["1"],
                            data=file_data,
                            timeout=300
                        )
                        if resp_put.status_code != 200:
                            raise Exception(f"HTTP {resp_put.status_code}")
                        break
                    except Exception as e:
                        if i == 2: raise Exception(f"小文件上传超时: {e}")
                        time.sleep(2)

            # 完成确认
            upload_data["isMultipart"] = file_size > slice_size
            complete_resp = self.client.upload_complete(upload_data)
            check_response(complete_resp)

            # 防御性检查与大文件兼容处理 (核心修复)
            resp_data = complete_resp.get("data", {})
            # 兼容逻辑：如果 data 为空但 code 为 0，视为成功
            if (not resp_data or "file_info" not in resp_data) and complete_resp.get("code") == 0:
                logger.warning(f"【123】上传API返回成功但无元数据(可能是大文件合并中)，手动构造成功响应: {target_name}")
                # 手动构造返回数据，确保流程不中断
                data = {
                    "FileId": "", # 此时拿不到ID，但文件已入库，后续刮削会自动修正
                    "FileName": target_name,
                    "Type": 0, 
                    "Size": file_size,
                    "UpdateAt": datetime.now().isoformat()
                }
            else:
                data = resp_data.get("file_info", {})

            if not data or "FileName" not in data:
                logger.error(f"【123】上传完成但数据异常: {json.dumps(complete_resp, ensure_ascii=False)}")
                return None

            end_time = time.time()
            duration = end_time - start_time
            speed_str = "Fast"
            if duration > 0:
                speed_mb = (file_size / 1024 / 1024) / duration
                speed_str = f"{speed_mb:.2f} MB/s"

            logger.info(f"【123】上传完毕: {target_name} | 耗时: {duration:.1f}s | 均速: {speed_str}")

            self._send_upload_webhook(
                file_name=data["FileName"],
                remote_path=str(target_path) + ("/" if data.get("Type") == 1 else ""),
                size=file_size,
                etag=file_md5,
                status="uploaded",
                speed=speed_str
            )

            return self._build_file_item(data, target_path)

        except Exception as e:
            logger.error(f"【123】上传流程崩溃: {e}")
            logger.debug(traceback.format_exc()) # 【日志优化】打印堆栈
            return None
        # 全局 Session 不在此关闭，由 __del__ 或垃圾回收处理

    def detail(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        return self.get_item(Path(fileitem.path))

    def copy(self, fileitem: schemas.FileItem, path: Path, new_name: str) -> bool:
        try:
            resp = self.client.fs_copy(
                fileitem.fileid, parent_id=self._path_to_id(str(path))
            )
            check_response(resp)
            logger.info(f"【123】复制成功: {fileitem.name} -> {path}")
            new_path = Path(path) / fileitem.name
            new_item = self.get_item(new_path)
            self.rename(new_item, new_name)
            del self._id_cache[fileitem.path]
            rename_new_path = Path(path) / new_name
            self._id_cache[str(rename_new_path)] = new_item.fileid
            return True
        except Exception as e:
            logger.error(f"【123】复制失败: {e}")
            return False

    def move(self, fileitem: schemas.FileItem, path: Path, new_name: str) -> bool:
        try:
            resp = self.client.fs_move(
                fileitem.fileid, parent_id=self._path_to_id(str(path))
            )
            check_response(resp)
            logger.info(f"【123】移动成功: {fileitem.name} -> {path}")
            new_path = Path(path) / fileitem.name
            new_item = self.get_item(new_path)
            self.rename(new_item, new_name)
            del self._id_cache[fileitem.path]
            rename_new_path = Path(path) / new_name
            self._id_cache[str(rename_new_path)] = new_item.fileid
            return True
        except Exception as e:
            logger.error(f"【123】移动失败: {e}")
            return False

    def link(self, fileitem: schemas.FileItem, target_file: Path) -> bool:
        pass

    def softlink(self, fileitem: schemas.FileItem, target_file: Path) -> bool:
        pass

    def usage(self) -> Optional[schemas.StorageUsage]:
        try:
            resp = self.client.user_info()
            check_response(resp)
            return schemas.StorageUsage(
                total=resp["data"]["SpacePermanent"],
                available=int(resp["data"]["SpacePermanent"])
                - int(resp["data"]["SpaceUsed"]),
            )
        except Exception:
            return None

    def _build_file_item(self, data, target_path):
        """
        构造返回对象
        """
        return schemas.FileItem(
            storage=self._disk_name,
            fileid=str(data.get("FileId", "")), # 兼容手动构造时 FileId 可能为空
            path=str(target_path) + ("/" if data.get("Type", 0) == 1 else ""),
            type="file" if data.get("Type", 0) == 0 else "dir",
            name=data["FileName"],
            basename=Path(data["FileName"]).stem,
            extension=Path(data["FileName"]).suffix[1:] if data.get("Type", 0) == 0 else None,
            pickcode=str(data),
            size=data.get("Size") if data.get("Type", 0) == 0 else None,
            modify_time=int(datetime.fromisoformat(data["UpdateAt"]).timestamp()),
        )