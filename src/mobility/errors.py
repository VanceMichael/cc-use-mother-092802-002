"""领域异常。"""

from __future__ import annotations


class MobilityDomainError(Exception):
    """客流归集领域错误基类。"""


class ValidationError(MobilityDomainError):
    """报送内容未通过校验（单位、覆盖日期、口径归属等）。"""


class ContainmentError(MobilityDomainError):
    """分项包含关系不成立，例如公路合计 != 营业性 + 非营业性小客车。"""


class QuarantineError(MobilityDomainError):
    """批次编号相同但内容不同，已隔离，等待裁决。

    quarantined_id 为隔离区内部分配的标识，原编号保留在 display_id。
    """

    def __init__(self, message: str, quarantined_id: str, display_id: str, conflicting_with: str):
        super().__init__(message)
        self.quarantined_id = quarantined_id
        self.display_id = display_id
        self.conflicting_with = conflicting_with


class WorkflowError(MobilityDomainError):
    """流程约束被违反，例如发布后原地覆盖、自己复核自己的调整。"""


class NotFoundError(MobilityDomainError):
    """引用的实体不存在。"""
