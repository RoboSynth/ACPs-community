"""
ADP (Agent Discovery Protocol) 错误码定义与错误处理工具

基于 ACPs-spec-ADP 协议规范定义的错误码。
"""

from enum import IntEnum
from typing import Any, Dict, Optional


# =============================================================================
# ADP 错误码枚举
# =============================================================================


class ADPErrorCode(IntEnum):
    """
    ADP 协议错误码枚举。

    命名规则：HTTP 状态码前缀 + 两位序号。
    - 307xx: 重定向相关
    - 400xx: 客户端请求参数错误
    - 401xx: 认证相关
    - 429xx: 限流相关
    - 508xx: 转发链路相关
    - 500xx: 服务端内部错误
    """

    # ── 307 Temporary Redirect ──
    CAPACITY_REDIRECT = 30701
    """CapacityRedirect: 当前发现服务器负载/维护受限，307 重定向到同权威机构的候选服务器。"""

    REGION_REDIRECT = 30702
    """RegionRedirect: 因地域或合规策略不覆盖该查询，307 指引到覆盖区域的发现服务器。"""

    MAX_REDIRECTS_EXCEEDED = 30703
    """MaxRedirectsExceeded: 客户端连续重定向次数超过限制（建议最大 5 次），请求被拒绝。"""

    # ── 400 Bad Request ──
    MISSING_QUERY = 40001
    """MissingQuery: type=explicit 时缺少 query，或文本为空字符串。"""

    FORWARD_DEPTH_LIMIT_INVALID = 40002
    """ForwardDepthLimitInvalid: forwardDepthLimit 不在 1-5 区间。"""

    FORWARD_CHAIN_INVALID = 40003
    """ForwardChainInvalid: 客户端携带的 forwardChain 包含非法 AIC。"""

    FILTER_INVALID = 40004
    """FilterInvalid: filter 中的条件不合法（如不支持的字段路径、运算符与字段类型不兼容、嵌套深度超限），或条件互相矛盾。"""

    FORWARD_FANOUT_LIMIT_INVALID = 40005
    """ForwardFanoutLimitInvalid: forwardFanoutLimit 不在 1-5 区间，或小于 1。"""

    # ── 401 Unauthorized ──
    CERTIFICATE_INVALID = 40101
    """CertificateInvalid: mTLS 证书无效、过期或被吊销。"""

    # ── 429 Too Many Requests ──
    CALLER_RATE_LIMITED = 42901
    """CallerRateLimited: 针对单个请求智能体的调用频率超限，需遵守 Retry-After。"""

    TENANT_RATE_LIMITED = 42902
    """TenantRateLimited: 上层租户/组织的配额已用尽，需遵守 Retry-After。"""

    # ── 508 Loop Detected ──
    FORWARD_LOOP_DETECTED = 50801
    """ForwardLoopDetected: 在 forwardChain 中发现自身 AIC，判定存在环路。"""

    FORWARD_DEPTH_EXCEEDED = 50802
    """ForwardDepthExceeded: 转发深度达到 forwardDepthLimit，不再继续转发。"""

    FORWARD_CHAIN_TAMPERED = 50803
    """ForwardChainTampered: 校验 forwardChain 最后一个 AIC 与当前请求证书内嵌 AIC 不同，链条完整性受到破坏。"""

    FORWARD_FANOUT_EXCEEDED = 50804
    """ForwardFanoutExceeded: 聚合转发分支数量超过剩余额度，或剩余额度分配之和超出 forwardFanoutLimit。"""

    FORWARD_SIGNATURE_INVALID = 50805
    """ForwardSignatureInvalid: forwardSignatures 中的签名验证失败，可能被篡改或签名不匹配。"""

    # ── 500 Internal Server Error ──
    INTERNAL_ERROR = 50001
    """InternalError: 发现服务器自身异常，无法完成查询。"""

    def is_redirect(self) -> bool:
        """判断该错误码是否表示重定向（30701, 30702）。"""
        return self.value in (30701, 30702)

    def is_retryable(self) -> bool:
        """判断该错误码是否表示可重试（限流类: 42901, 42902）。"""
        return self.value in (42901, 42902)

    def is_client_error(self) -> bool:
        """判断该错误码是否为客户端参数错误（400xx）。"""
        return 40000 <= self.value < 40100

    def is_forward_error(self) -> bool:
        """判断该错误码是否为转发链路错误（508xx）。"""
        return 50800 <= self.value < 50900


# =============================================================================
# 错误码 → 友好名称映射
# =============================================================================

ADP_ERROR_NAMES: Dict[int, str] = {
    ADPErrorCode.CAPACITY_REDIRECT: "CapacityRedirect",
    ADPErrorCode.REGION_REDIRECT: "RegionRedirect",
    ADPErrorCode.MAX_REDIRECTS_EXCEEDED: "MaxRedirectsExceeded",
    ADPErrorCode.MISSING_QUERY: "MissingQuery",
    ADPErrorCode.FORWARD_DEPTH_LIMIT_INVALID: "ForwardDepthLimitInvalid",
    ADPErrorCode.FORWARD_CHAIN_INVALID: "ForwardChainInvalid",
    ADPErrorCode.FILTER_INVALID: "FilterInvalid",
    ADPErrorCode.FORWARD_FANOUT_LIMIT_INVALID: "ForwardFanoutLimitInvalid",
    ADPErrorCode.CERTIFICATE_INVALID: "CertificateInvalid",
    ADPErrorCode.CALLER_RATE_LIMITED: "CallerRateLimited",
    ADPErrorCode.TENANT_RATE_LIMITED: "TenantRateLimited",
    ADPErrorCode.FORWARD_LOOP_DETECTED: "ForwardLoopDetected",
    ADPErrorCode.FORWARD_DEPTH_EXCEEDED: "ForwardDepthExceeded",
    ADPErrorCode.FORWARD_CHAIN_TAMPERED: "ForwardChainTampered",
    ADPErrorCode.FORWARD_FANOUT_EXCEEDED: "ForwardFanoutExceeded",
    ADPErrorCode.FORWARD_SIGNATURE_INVALID: "ForwardSignatureInvalid",
    ADPErrorCode.INTERNAL_ERROR: "InternalError",
}

# =============================================================================
# 错误码 → HTTP 状态码映射
# =============================================================================

ADP_ERROR_HTTP_STATUS: Dict[int, int] = {
    ADPErrorCode.CAPACITY_REDIRECT: 307,
    ADPErrorCode.REGION_REDIRECT: 307,
    ADPErrorCode.MAX_REDIRECTS_EXCEEDED: 400,
    ADPErrorCode.MISSING_QUERY: 400,
    ADPErrorCode.FORWARD_DEPTH_LIMIT_INVALID: 400,
    ADPErrorCode.FORWARD_CHAIN_INVALID: 400,
    ADPErrorCode.FILTER_INVALID: 400,
    ADPErrorCode.FORWARD_FANOUT_LIMIT_INVALID: 400,
    ADPErrorCode.CERTIFICATE_INVALID: 401,
    ADPErrorCode.CALLER_RATE_LIMITED: 429,
    ADPErrorCode.TENANT_RATE_LIMITED: 429,
    ADPErrorCode.FORWARD_LOOP_DETECTED: 508,
    ADPErrorCode.FORWARD_DEPTH_EXCEEDED: 508,
    ADPErrorCode.FORWARD_CHAIN_TAMPERED: 508,
    ADPErrorCode.FORWARD_FANOUT_EXCEEDED: 508,
    ADPErrorCode.FORWARD_SIGNATURE_INVALID: 508,
    ADPErrorCode.INTERNAL_ERROR: 500,
}


# =============================================================================
# ADP 异常类
# =============================================================================


class ADPError(Exception):
    """
    ADP 协议异常基类。

    Attributes:
        code: ADP 错误码（ADPErrorCode 枚举值）。
        message: 错误描述信息。
        data: 可选的附加错误数据。
        http_status: 对应的 HTTP 状态码。
    """

    def __init__(
        self,
        code: ADPErrorCode,
        message: Optional[str] = None,
        data: Optional[Any] = None,
    ):
        self.code = code
        self.message = message or ADP_ERROR_NAMES.get(code, "UnknownError")
        self.data = data
        self.http_status = ADP_ERROR_HTTP_STATUS.get(code, 500)
        super().__init__(self.message)

    def to_error_body(self) -> Dict[str, Any]:
        """
        转换为 ADP 响应体中的 error 对象结构。

        Returns:
            符合 CommonResponse.error 结构的字典。
        """
        error: Dict[str, Any] = {
            "code": int(self.code),
            "message": self.message,
        }
        if self.data is not None:
            error["data"] = self.data
        return error

    def to_response_dict(self) -> Dict[str, Any]:
        """
        转换为完整的 ADP 错误响应体字典。

        Returns:
            符合 CommonResponse 结构的字典，包含 error 字段。
        """
        return {"error": self.to_error_body()}

    def is_redirect(self) -> bool:
        """判断该错误是否表示重定向。"""
        try:
            return ADPErrorCode(self.code).is_redirect()
        except ValueError:
            return False

    def is_retryable(self) -> bool:
        """判断该错误是否可重试（限流类）。"""
        try:
            return ADPErrorCode(self.code).is_retryable()
        except ValueError:
            return False

    def is_client_error(self) -> bool:
        """判断该错误是否为客户端参数错误。"""
        try:
            return ADPErrorCode(self.code).is_client_error()
        except ValueError:
            return False

    def is_forward_error(self) -> bool:
        """判断该错误是否为转发链路错误。"""
        try:
            return ADPErrorCode(self.code).is_forward_error()
        except ValueError:
            return False

    def __repr__(self) -> str:
        return (
            f"ADPError(code={int(self.code)}, "
            f"message={self.message!r}, "
            f"http_status={self.http_status})"
        )


# =============================================================================
# 便捷工厂函数
# =============================================================================


def make_error_response(
    code: ADPErrorCode,
    message: Optional[str] = None,
    data: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    快速构建 ADP 错误响应体字典。

    Args:
        code: ADP 错误码。
        message: 可选的错误描述，缺省使用错误码对应的默认名称。
        data: 可选的附加错误数据。

    Returns:
        符合 CommonResponse 结构的字典。

    Example:
        >>> make_error_response(ADPErrorCode.MISSING_QUERY)
        {'error': {'code': 40001, 'message': 'MissingQuery'}}

        >>> make_error_response(
        ...     ADPErrorCode.FORWARD_FANOUT_EXCEEDED,
        ...     message="Fan-out budget exhausted",
        ...     data={"availableBudget": 1, "requiredBranches": 3},
        ... )
        {'error': {'code': 50804, 'message': 'Fan-out budget exhausted', 'data': {'availableBudget': 1, 'requiredBranches': 3}}}
    """
    return ADPError(code=code, message=message, data=data).to_response_dict()


def get_http_status_for_error(code: ADPErrorCode) -> int:
    """
    获取 ADP 错误码对应的 HTTP 状态码。

    Args:
        code: ADP 错误码。

    Returns:
        对应的 HTTP 状态码。
    """
    return ADP_ERROR_HTTP_STATUS.get(code, 500)
