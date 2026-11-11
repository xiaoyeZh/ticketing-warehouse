"""只向 CLI/任务表暴露可控错误码，不传播连接参数或源数据。"""


class ExtractionError(RuntimeError):
    def __init__(self, code, batch_id=None):
        self.code = code
        self.batch_id = str(batch_id) if batch_id is not None else None
        super().__init__(code)
