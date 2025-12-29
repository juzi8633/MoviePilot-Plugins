import ast
import time
import threading
import copy
import mmap
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Dict
from hashlib import md5
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from p123client import P123Client, check_response

from app import schemas
from app.log import logger
from app.core.config import settings, global_vars
from app.modules.filemanager.storages import transfer_process
from app.utils.string import StringUtils


class P123Api:
    """
    123云盘基础操作 (终极优化版：异步回调+指数重试+MD5进度)
    """

    # FileId和路径缓存
    _id_cache: Dict[str, str] = {}

    def __init__(self, client: P123Client, disk_name: str, 
                 webhook_url: str = None, 
                 webhook_secret: str = None, 
                 upload_threads: str = None):
        self.client = client
        self._disk_name = disk_name
        self.webhook_url = webhook_url
        self.webhook_secret = webhook_secret
        
        # 处理并发线程数
        try:
            self.upload_threads = int(upload_threads) if upload_threads else 3
            if self.upload_threads < 1: self.upload_threads = 1
            if self.upload_threads > 32: self.upload_threads = 32 # 限制最大线程
        except Exception:
            self.upload_threads = 3

    def _path_to_id(self, path: str):
        """
        通过路径获取ID
        """
        # 根目录
        if path == "/":
            return "0"
        if len(path) > 1 and path.endswith("/"):
            path = path[:-1]
        # 检查缓存
        if path in self._id_cache:
            return self._id_cache[path]
        # 逐级查找缓存
        current_id = 0
        parent_path = "/"
        for p in Path(path).parents:
            if str(p) in self._id_cache:
                parent_path = str(p)
                current_id = self._id_cache[parent_path]
                break
        # 计算相对路径
        rel_path = Path(path).relative_to(parent_path)
        for part in Path(rel_path).parts:
            find_part = False
            page = 1
            _next = 0
            first_find = True
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
                resp = self.client.fs_list(payload)
                check_response(resp)
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
                raise FileNotFoundError(f"【123】{path} 不存在")
        if not current_id:
            raise FileNotFoundError(f"【123】{path} 不存在")
        # 缓存路径
        self._id_cache[path] = str(current_id)
        return str(current_id)

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
                resp = self.client.fs_list(payload)
                check_response(resp)
                item_list = resp.get("data").get("InfoList")
                if not item_list:
                    break
                for item in item_list:
                    path = f"{fileitem.path}{item['FileName']}"
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
            resp = self.client.fs_mkdir(name, parent_id=self._path_to_id(fileitem.path))
            check_response(resp)
            logger.debug(f"【123】创建目录: {resp}")
            data = resp["data"]["Info"]
            # 缓存新目录
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
            """
            查找下级目录中匹配名称的目录
            """
            for sub_folder in self.list(_fileitem):
                if sub_folder.type != "dir":
                    continue
                if sub_folder.name == _name:
                    return sub_folder
            return None

        # 是否已存在
        folder = self.get_item(path)
        if folder:
            return folder
        # 逐级查找和创建目录
        fileitem = schemas.FileItem(storage=self._disk_name, path="/")
        for part in path.parts[1:]:
            dir_file = __find_dir(fileitem, part)
            if dir_file:
                fileitem = dir_file
            else:
                dir_file = self.create_folder(fileitem, part)
                if not dir_file:
                    logger.warn(f"【123】创建目录 {fileitem.path}{part} 失败！")
                    return None
                fileitem = dir_file
        return fileitem

    def get_item(self, path: Path) -> Optional[schemas.FileItem]:
        """
        获取文件或目录，不存在返回None
        """
        try:
            file_id = self._path_to_id(str(path))
            if not file_id:
                return None
            resp = self.client.fs_info(int(file_id))
            check_response(resp)
            logger.debug(f"【123】获取文件信息: {resp}")
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
            logger.debug(f"【123】获取文件信息失败: {str(e)}")
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
            resp = self.client.fs_trash(int(fileitem.fileid), event="intoRecycle")
            check_response(resp)
            logger.debug(f"【123】删除文件: {resp}")
            return True
        except Exception:
            return False

    def rename(self, fileitem: schemas.FileItem, name: str) -> bool:
        """
        重命名文件
        """
        try:
            payload = {
                "FileId": int(fileitem.fileid),
                "fileName": name,
                "duplicate": 2,
            }
            resp = self.client.fs_rename(payload)
            check_response(resp)
            logger.debug(f"【123】重命名文件: {resp}")
            return True
        except Exception:
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
        except Exception as e:
            logger.error(f"【123】获取下载链接失败: {fileitem.name} - {str(e)}")
            return None

        # 获取文件大小
        file_size = fileitem.size

        # 初始化进度条
        logger.info(f"【123】开始下载: {fileitem.name} -> {local_path}")
        progress_callback = transfer_process(Path(fileitem.path).as_posix())

        try:
            with requests.get(download_url, stream=True) as r:
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
                            # 更新进度
                            if file_size:
                                progress = (downloaded_size * 100) / file_size
                                progress_callback(progress)

                # 完成下载
                progress_callback(100)
                logger.info(f"【123】下载完成: {fileitem.name}")

        except requests.exceptions.RequestException as e:
            logger.error(f"【123】下载网络错误: {fileitem.name} - {str(e)}")
            if local_path.exists():
                local_path.unlink()
            return None
        except Exception as e:
            logger.error(f"【123】下载失败: {fileitem.name} - {str(e)}")
            if local_path.exists():
                local_path.unlink()
            return None

        return local_path

    def _send_upload_webhook(self, file_name: str, remote_path: str, size: int, etag: str, status: str, speed: str = ""):
        """
        发送上传完成 Webhook (异步非阻塞)
        """
        if not self.webhook_url:
            return

        def _do_send():
            try:
                logger.info(f"【123】WebHook触发 | 文件: {file_name} | 状态: {status}")
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
                resp = requests.post(self.webhook_url, json=payload, timeout=10)
                logger.info(f"【123】WebHook响应 [{resp.status_code}]: {resp.text[:200]}")
                if resp.status_code != 200:
                    logger.warning(f"【123】WebHook状态码异常: {resp.status_code}")
            except Exception as e:
                logger.warning(f"【123】WebHook发送失败: {e}")

        # [优化] 启动守护线程发送，不阻塞主流程
        threading.Thread(target=_do_send, daemon=True).start()

    def _upload_chunk_worker(self, session, mmapped_file, slice_no, offset, size, upload_data, target_name):
        """
        分片上传的工作线程函数
        """
        try:
            chunk = mmapped_file[offset : offset + size]
        except Exception as e:
            logger.error(f"【123】mmap读取失败(Offset:{offset}): {e}")
            raise e
        
        if not chunk:
            return 0

        current_upload_data = copy.deepcopy(upload_data)
        current_upload_data["partNumberStart"] = slice_no
        current_upload_data["partNumberEnd"] = slice_no + 1

        max_retries = 5
        retry_count = 0
        
        # 获取URL
        current_upload_url_resp = self.client.upload_prepare(current_upload_data)
        check_response(current_upload_url_resp)

        while retry_count < max_retries:
            try:
                upload_url = current_upload_url_resp["data"]["presignedUrls"][str(slice_no)]
                session.put(
                    upload_url,
                    data=chunk,
                    headers={"authorization": ""},
                    timeout=300
                )
                return len(chunk)

            except Exception as upload_err:
                retry_count += 1
                if retry_count < max_retries:
                    # [优化] 指数退避策略：1s, 2s, 4s, 8s...
                    wait_time = 2 ** (retry_count - 1)
                    logger.warning(f"【123】{target_name} 分片{slice_no} 重试({retry_count}/{max_retries}) 等待{wait_time}s")
                    time.sleep(wait_time)
                    try:
                        current_upload_url_resp = self.client.upload_prepare(current_upload_data)
                        check_response(current_upload_url_resp)
                    except Exception:
                        pass
                else:
                    logger.error(f"【123】{target_name} 分片{slice_no} 失败: {upload_err}")
                    raise upload_err
        return 0

    def upload(
        self,
        target_dir: schemas.FileItem,
        local_path: Path,
        new_name: Optional[str] = None,
    ) -> Optional[schemas.FileItem]:
        """
        上传文件 (终极优化版)
        """
        start_time = time.time()
        target_name = new_name or local_path.name
        target_path = Path(target_dir.path) / target_name
        file_size = local_path.stat().st_size

        logger.info(f"【123】开始处理文件: {target_name} ({StringUtils.str_filesize(file_size)})")

        # 1. 计算MD5 (带进度心跳)
        file_md5 = ""
        try:
            with open(local_path, "rb") as f:
                hash_md5 = md5()
                processed_size = 0
                last_log_time = time.time()
                
                # 4MB Buffer
                for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
                    hash_md5.update(chunk)
                    processed_size += len(chunk)
                    
                    # [优化] MD5计算进度心跳 (每5秒或大文件每10%)
                    if time.time() - last_log_time > 5 and file_size > 1024*1024*100:
                        pct = int(processed_size / file_size * 100)
                        logger.info(f"【123】正在计算特征值: {pct}%")
                        last_log_time = time.time()
                        
                file_md5 = hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"【123】文件读取失败: {e}")
            return None

        try:
            # 2. 准备参数 & 申请秒传 (移除SliceSize参数)
            upload_req_payload = {
                "etag": file_md5,
                "fileName": target_name,
                "size": file_size,
                "parentFileId": int(target_dir.fileid),
                "type": 0,
                "duplicate": 2,
            }
            
            resp = self.client.upload_request(upload_req_payload)
            check_response(resp)
            
            # === 场景1：秒传成功 ===
            if resp.get("data").get("Reuse"):
                logger.info(f"【123】秒传成功: {target_name}")
                data = resp.get("data", {}).get("Info", {})
                
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
        upload_session = requests.Session()
        try:
            upload_data = resp.get("data")
            if not upload_data:
                logger.error(f"【123】上传响应异常: {json.dumps(resp, ensure_ascii=False)}")
                return None
                
            slice_size = int(upload_data.get("SliceSize", 10*1024*1024))
            
            # 配置连接池 (统一用于大文件和小文件)
            adapter = HTTPAdapter(
                pool_connections=self.upload_threads, 
                pool_maxsize=self.upload_threads,
                max_retries=2
            )
            upload_session.mount('https://', adapter)
            upload_session.mount('http://', adapter)

            if file_size > slice_size:
                logger.info(
                    f"【123】启动并发上传 | 线程: {self.upload_threads} | 分片: {StringUtils.str_filesize(slice_size)} | 总分片数: {int(file_size/slice_size)+1}"
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
                
                with open(local_path, "rb") as f:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                        with ThreadPoolExecutor(max_workers=self.upload_threads) as executor:
                            future_to_slice = {
                                executor.submit(
                                    self._upload_chunk_worker, 
                                    upload_session, mmapped_file, sno, off, sz, upload_data, target_name
                                ): sno for sno, off, sz in tasks
                            }
                            
                            for future in as_completed(future_to_slice):
                                if global_vars.is_transfer_stopped(local_path.as_posix()):
                                    logger.info(f"【123】任务终止: {target_name}")
                                    executor.shutdown(wait=False)
                                    return None
                                
                                try:
                                    bytes_uploaded = future.result()
                                    downloaded_size += bytes_uploaded
                                    
                                    # 控制台/日志心跳
                                    current_time = time.time()
                                    if current_time - last_log_time > 15:
                                        percent = int((downloaded_size / file_size) * 100)
                                        logger.info(f"【123】上传进度: {percent}% ({StringUtils.str_filesize(downloaded_size)}/{StringUtils.str_filesize(file_size)})")
                                        last_log_time = current_time

                                    if file_size:
                                        progress_callback((downloaded_size * 100) / file_size)
                                except Exception as e:
                                    logger.error(f"【123】分片异常: {e}")
                                    raise e

                progress_callback(100)
            else:
                # [优化] 小文件直传也走Session连接池
                logger.info(f"【123】小文件直传: {target_name}")
                resp = self.client.upload_auth(upload_data)
                check_response(resp)
                
                with open(local_path, "rb") as f:
                    file_data = f.read()
                
                for i in range(3):
                    try:
                        # 使用 session.put 替代 requests.put
                        upload_session.put(
                            resp["data"]["presignedUrls"]["1"],
                            data=file_data,
                            timeout=300
                        )
                        break
                    except Exception as e:
                        if i == 2: raise Exception(f"小文件上传超时: {e}")
                        time.sleep(2)

            # 完成确认
            upload_data["isMultipart"] = file_size > slice_size
            complete_resp = self.client.upload_complete(upload_data)
            check_response(complete_resp)

            data = complete_resp.get("data", {}).get("file_info", {})
            if not data or "FileName" not in data:
                logger.error(f"【123】上传数据异常: {json.dumps(complete_resp, ensure_ascii=False)}")
                return None

            # 计算最终统计
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
            logger.error(f"【123】上传失败 [{target_name}]: {e}")
            return None
        finally:
            upload_session.close()

    def _build_file_item(self, data, target_path):
        """
        构造返回对象
        """
        return schemas.FileItem(
            storage=self._disk_name,
            fileid=str(data["FileId"]),
            path=str(target_path) + ("/" if data["Type"] == 0 else ""),
            type="file" if data["Type"] == 0 else "dir",
            name=data["FileName"],
            basename=Path(data["FileName"]).stem,
            extension=Path(data["FileName"]).suffix[1:] if data["Type"] == 0 else None,
            pickcode=str(data),
            size=data["Size"] if data["Type"] == 0 else None,
            modify_time=int(datetime.fromisoformat(data["UpdateAt"]).timestamp()),
        )

    # 其他方法保持不变
    def detail(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        return self.get_item(Path(fileitem.path))

    def copy(self, fileitem: schemas.FileItem, path: Path, new_name: str) -> bool:
        try:
            resp = self.client.fs_copy(
                fileitem.fileid, parent_id=self._path_to_id(str(path))
            )
            check_response(resp)
            logger.debug(f"【123】复制文件: {resp}")
            new_path = Path(path) / fileitem.name
            new_item = self.get_item(new_path)
            self.rename(new_item, new_name)
            del self._id_cache[fileitem.path]
            rename_new_path = Path(path) / new_name
            self._id_cache[str(rename_new_path)] = new_item.fileid
            return True
        except Exception:
            return False

    def move(self, fileitem: schemas.FileItem, path: Path, new_name: str) -> bool:
        try:
            resp = self.client.fs_move(
                fileitem.fileid, parent_id=self._path_to_id(str(path))
            )
            check_response(resp)
            logger.debug(f"【123】移动文件: {resp}")
            new_path = Path(path) / fileitem.name
            new_item = self.get_item(new_path)
            self.rename(new_item, new_name)
            del self._id_cache[fileitem.path]
            rename_new_path = Path(path) / new_name
            self._id_cache[str(rename_new_path)] = new_item.fileid
            return True
        except Exception:
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