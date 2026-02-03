import time
import threading
from p123client import P123Client
from app.log import logger

class P123AutoClient:
    """
    123云盘客户端 (线程安全 & 自动保活)
    负责处理底层 API 调用、Token 自动刷新和并发锁控制
    """
    _last_reset_time = 0

    def __init__(self, passport, password):
        self._client = None
        self._passport = passport
        self._password = password
        self._lock = threading.Lock()  # 核心：多线程上传时的Token刷新锁

    def __getattr__(self, name):
        # 懒加载初始化
        if self._client is None:
            with self._lock:
                if self._client is None:
                    # logger.info(f"【123】初始化 P123Client 实例...")
                    self._client = P123Client(self._passport, self._password)

        def wrapped(*args, **kwargs):
            # 获取底层客户端方法
            attr = getattr(self._client, name)
            if not callable(attr):
                return attr
            
            # 执行调用
            result = attr(*args, **kwargs)
            
            # 捕获 Token 失效 (401)
            # 123云盘的特征错误：code=401, message="tokens number has exceeded the limit"
            if (
                isinstance(result, dict)
                and result.get("code") == 401
                and result.get("message") == "tokens number has exceeded the limit"
            ):
                with self._lock:
                    # 防止并发线程重复刷新 (10秒冷却)
                    if time.time() - self._last_reset_time < 10:
                        logger.debug("【123】Token刷新冷却中，使用现有实例重试")
                    else:
                        logger.warning(f"【123】Token失效，正在重新登录...")
                        try:
                            self._client = P123Client(self._passport, self._password)
                            self._last_reset_time = time.time()
                            logger.info("【123】重新登录成功")
                        except Exception as e:
                            logger.error(f"【123】重新登录失败: {e}")
                            return result

                # 获取新实例的方法并重试
                attr = getattr(self._client, name)
                if callable(attr):
                    logger.info(f"【123】重试请求: {name}")
                    return attr(*args, **kwargs)
                
            return result

        return wrapped
