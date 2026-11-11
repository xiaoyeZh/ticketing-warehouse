"""只向 CLI/任务表暴露可控错误码，不传播连接参数或源数据。"""


class ExtractionError(RuntimeError):
    def __init__(self, code, batch_id=None):
        self.code = code
        self.batch_id = str(batch_id) if batch_id is not None else None
        super().__init__(code)


class TransformationError(RuntimeError):
    def __init__(self, code, source_batch_id=None, run_id=None):
        self.code = code
        self.source_batch_id = str(source_batch_id) if source_batch_id is not None else None
        self.run_id = str(run_id) if run_id is not None else None
        super().__init__(code)


class WarehouseError(RuntimeError):
    def __init__(self, code, **identifiers):
        self.code = code
        self.identifiers = {key: str(value) for key, value in identifiers.items() if value is not None}
        super().__init__(code)
