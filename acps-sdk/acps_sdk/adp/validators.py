"""
ADP (Agent Discovery Protocol) 验证工具

提供转发链、扇出额度、过滤条件等的验证逻辑。
基于 ACPs-spec-ADP 协议规范。
"""

from __future__ import annotations

from copy import deepcopy
from typing import List, Optional

from .constants import (
    FILTER_MAX_NESTING_DEPTH,
    FORWARD_DEPTH_LIMIT_DEFAULT,
    FORWARD_DEPTH_LIMIT_MAX,
    FORWARD_DEPTH_LIMIT_MIN,
    FORWARD_FANOUT_LIMIT_DEFAULT,
    FORWARD_FANOUT_LIMIT_MAX,
    FORWARD_FANOUT_LIMIT_MIN,
    QUERY_TYPE_EXPLICIT,
)
from .errors import ADPError, ADPErrorCode
from .models import DiscoveryFilter, DiscoveryRequest


# =============================================================================
# 请求参数验证
# =============================================================================


def validate_discovery_request(request: DiscoveryRequest) -> None:
    """
    对 DiscoveryRequest 进行业务规则校验。

    检查内容：
    - type=explicit 时 query 不能为空（已由模型层验证，此处做二次防护）。
    - forwardDepthLimit 在 [1, 5] 区间。
    - forwardFanoutLimit 在 [1, 5] 区间。
    - filter 嵌套深度不超过建议上限。

    Args:
        request: 待验证的发现请求。

    Raises:
        ADPError: 校验不通过时抛出对应错误码的异常。
    """
    # 1. query 校验
    if request.type == QUERY_TYPE_EXPLICIT:
        if not request.query or not request.query.strip():
            raise ADPError(
                code=ADPErrorCode.MISSING_QUERY,
                message="type='explicit' 时 query 字段必填且不能为空字符串",
            )

    # 2. forwardDepthLimit 范围校验
    if request.forward_depth_limit is not None:
        if not (
            FORWARD_DEPTH_LIMIT_MIN
            <= request.forward_depth_limit
            <= FORWARD_DEPTH_LIMIT_MAX
        ):
            raise ADPError(
                code=ADPErrorCode.FORWARD_DEPTH_LIMIT_INVALID,
                message=(
                    f"forwardDepthLimit 必须在 "
                    f"[{FORWARD_DEPTH_LIMIT_MIN}, {FORWARD_DEPTH_LIMIT_MAX}] 区间，"
                    f"收到: {request.forward_depth_limit}"
                ),
            )

    # 3. forwardFanoutLimit 范围校验
    if request.forward_fanout_limit is not None:
        if not (
            FORWARD_FANOUT_LIMIT_MIN
            <= request.forward_fanout_limit
            <= FORWARD_FANOUT_LIMIT_MAX
        ):
            raise ADPError(
                code=ADPErrorCode.FORWARD_FANOUT_LIMIT_INVALID,
                message=(
                    f"forwardFanoutLimit 必须在 "
                    f"[{FORWARD_FANOUT_LIMIT_MIN}, {FORWARD_FANOUT_LIMIT_MAX}] 区间，"
                    f"收到: {request.forward_fanout_limit}"
                ),
            )

    # 4. filter 嵌套深度校验
    if request.filter is not None:
        _validate_filter_depth(request.filter, current_depth=1)


def _validate_filter_depth(
    filter_obj: DiscoveryFilter,
    current_depth: int,
) -> None:
    """
    递归校验过滤条件嵌套深度。

    Args:
        filter_obj: 过滤条件对象。
        current_depth: 当前嵌套层级（从 1 开始）。

    Raises:
        ADPError: 嵌套深度超限时抛出 FILTER_INVALID 错误。
    """
    if current_depth > FILTER_MAX_NESTING_DEPTH:
        raise ADPError(
            code=ADPErrorCode.FILTER_INVALID,
            message=(
                f"过滤条件嵌套深度超过建议上限 {FILTER_MAX_NESTING_DEPTH} 层，"
                f"当前深度: {current_depth}"
            ),
        )
    if filter_obj.groups:
        for sub_group in filter_obj.groups:
            _validate_filter_depth(sub_group, current_depth + 1)


# =============================================================================
# 转发链验证
# =============================================================================


def validate_forward_chain(
    request: DiscoveryRequest,
    current_server_aic: str,
    sender_aic: Optional[str] = None,
) -> None:
    """
    验证转发链的完整性和合法性。

    检查内容：
    1. 环路检测：当前服务器 AIC 不能已经在 forwardChain 中。
    2. 深度检查：forwardChain 长度 < forwardDepthLimit。
    3. 链条完整性：forwardChain 最后一个 AIC 应与发送方(sender_aic)一致。

    Args:
        request: 发现请求。
        current_server_aic: 当前发现服务器的 AIC。
        sender_aic: 发送方(上一跳)的 AIC（从 mTLS 证书中提取）。
                    对于首次收到请求（无 forwardChain）可为 None。

    Raises:
        ADPError: 校验不通过时抛出对应错误码的异常。
    """
    chain = request.forward_chain or []
    depth_limit = request.get_effective_depth_limit()

    # 1. 环路检测
    if current_server_aic in chain:
        raise ADPError(
            code=ADPErrorCode.FORWARD_LOOP_DETECTED,
            message=(
                f"在 forwardChain 中检测到当前服务器 AIC {current_server_aic!r}，"
                f"判定为环路。forwardChain: {chain}"
            ),
            data={"forwardChain": chain, "currentServerAic": current_server_aic},
        )

    # 2. 深度检查
    if len(chain) >= depth_limit:
        raise ADPError(
            code=ADPErrorCode.FORWARD_DEPTH_EXCEEDED,
            message=(
                f"转发深度已达到 forwardDepthLimit={depth_limit}，"
                f"当前 forwardChain 长度: {len(chain)}"
            ),
            data={
                "forwardChain": chain,
                "forwardDepthLimit": depth_limit,
            },
        )

    # 3. 链条完整性检查
    if chain and sender_aic is not None:
        last_aic = chain[-1]
        if last_aic != sender_aic:
            raise ADPError(
                code=ADPErrorCode.FORWARD_CHAIN_TAMPERED,
                message=(
                    f"forwardChain 最后一个 AIC {last_aic!r} 与发送方证书 AIC "
                    f"{sender_aic!r} 不一致，链条完整性受到破坏"
                ),
                data={
                    "forwardChainLastAic": last_aic,
                    "senderAic": sender_aic,
                },
            )


# =============================================================================
# 扇出额度验证
# =============================================================================


def validate_fanout_budget(
    request: DiscoveryRequest,
    required_branches: int,
) -> None:
    """
    验证当前请求的 fan-out 额度是否满足并发需求。

    Args:
        request: 发现请求。
        required_branches: 计划并发的下游分支数量。

    Raises:
        ADPError: 额度不足时抛出 FORWARD_FANOUT_EXCEEDED 错误。
    """
    remaining = request.get_effective_fanout_remaining()

    if remaining < required_branches:
        raise ADPError(
            code=ADPErrorCode.FORWARD_FANOUT_EXCEEDED,
            message=(
                f"fan-out 剩余额度 {remaining} 不足以支撑 "
                f"{required_branches} 个并发分支"
            ),
            data={
                "availableBudget": remaining,
                "requiredBranches": required_branches,
            },
        )


# =============================================================================
# 信任列表验证
# =============================================================================


def validate_trusted_target(
    request: DiscoveryRequest,
    target_aic: str,
) -> bool:
    """
    检查目标发现服务器是否在信任列表中。

    如果 forwardTrustedServers 为空或未提供，则无信任限制，返回 True。

    Args:
        request: 发现请求。
        target_aic: 目标发现服务器的 AIC。

    Returns:
        True 表示目标可信或无信任限制，False 表示目标不在信任列表中。
    """
    trusted = request.forward_trusted_servers
    if not trusted:
        return True
    return target_aic in trusted


# =============================================================================
# 转发超时检查
# =============================================================================


def should_continue_forwarding(request: DiscoveryRequest) -> bool:
    """
    根据超时配置判断是否应该继续转发。

    当 forwardTotalTimeoutMs 剩余值小于 forwardEachTimeoutMs 时，不应继续转发。

    Args:
        request: 发现请求。

    Returns:
        True 表示可以继续转发，False 表示不应继续。
    """
    total_remaining = request.get_effective_total_timeout_ms()
    each_timeout = request.get_effective_each_timeout_ms()
    return total_remaining >= each_timeout


# =============================================================================
# 转发请求构建工具
# =============================================================================


def build_forwarded_request(
    original: DiscoveryRequest,
    current_server_aic: str,
    fanout_remaining_for_branch: int,
    elapsed_ms: int = 0,
    signature: Optional[str] = None,
    trusted_servers: Optional[List[str]] = None,
) -> DiscoveryRequest:
    """
    基于原始请求构建转发给下游的请求。

    该方法会：
    1. 深拷贝原始请求。
    2. 将当前服务器 AIC 追加到 forwardChain。
    3. 更新 forwardFanoutRemaining。
    4. 动态调整 forwardTotalTimeoutMs（减去已用时间）。
    5. 若为首个节点且提供 trusted_servers，填充 forwardTrustedServers。
    6. 若提供 signature，追加到 forwardSignatures。

    Args:
        original: 原始发现请求。
        current_server_aic: 当前发现服务器的 AIC。
        fanout_remaining_for_branch: 分配给本分支的剩余扇出额度。
        elapsed_ms: 当前节点已消耗的时间（毫秒），用于调整总超时。
        signature: 当前节点的数字签名（可选）。
        trusted_servers: 信任的发现服务器 AIC 列表（仅首个节点需要提供）。

    Returns:
        适合转发给下游的新 DiscoveryRequest 实例。
    """
    # 深拷贝避免影响原始请求
    data = original.to_dict()
    forwarded = DiscoveryRequest.from_dict(data)

    # 1. 追加当前 AIC 到 forwardChain
    if forwarded.forward_chain is None:
        forwarded.forward_chain = []
    forwarded.forward_chain = list(forwarded.forward_chain) + [current_server_aic]

    # 2. 更新 forwardFanoutRemaining
    forwarded.forward_fanout_remaining = fanout_remaining_for_branch

    # 3. 动态调整 forwardTotalTimeoutMs
    effective_total = original.get_effective_total_timeout_ms()
    new_total = max(0, effective_total - elapsed_ms)
    forwarded.forward_total_timeout_ms = new_total

    # 4. 填充 forwardTrustedServers（仅首个节点）
    if trusted_servers is not None:
        forwarded.forward_trusted_servers = list(trusted_servers)

    # 5. 追加签名
    if signature is not None:
        if forwarded.forward_signatures is None:
            forwarded.forward_signatures = []
        forwarded.forward_signatures = list(forwarded.forward_signatures) + [signature]

    # 6. 确保 forwardDepthLimit 已设置
    if forwarded.forward_depth_limit is None:
        forwarded.forward_depth_limit = FORWARD_DEPTH_LIMIT_DEFAULT

    # 7. 确保 forwardFanoutLimit 已设置
    if forwarded.forward_fanout_limit is None:
        forwarded.forward_fanout_limit = FORWARD_FANOUT_LIMIT_DEFAULT

    return forwarded


def allocate_fanout_budget(
    total_remaining: int,
    branch_count: int,
    strategy: str = "equal",
    weights: Optional[List[int]] = None,
) -> List[int]:
    """
    分配 fan-out 额度到各分支。

    在进行聚合转发（fan-out）时，当前节点需要从 forwardFanoutRemaining 中
    为每个下游分支分配额度。所有下游收到的剩余额度之和（不含本次并发占用的 branch_count）
    不得超过节点接收时的剩余额度减去 branch_count。

    Args:
        total_remaining: 当前节点接收到的 forwardFanoutRemaining。
        branch_count: 计划并发的分支数量。
        strategy: 分配策略，"equal" 表示均分，"weighted" 表示按权重分配。
        weights: 权重列表，仅在 strategy="weighted" 时生效。

    Returns:
        每个分支被分配的 forwardFanoutRemaining 列表。

    Raises:
        ADPError: 额度不足时抛出错误。

    Example:
        >>> allocate_fanout_budget(total_remaining=4, branch_count=3)
        [0, 0, 1]
        >>> allocate_fanout_budget(total_remaining=4, branch_count=3, strategy="weighted", weights=[2, 1, 1])
        [0, 0, 1]
    """
    if total_remaining < branch_count:
        raise ADPError(
            code=ADPErrorCode.FORWARD_FANOUT_EXCEEDED,
            message=(
                f"fan-out 剩余额度 {total_remaining} 不足以支撑 "
                f"{branch_count} 个并发分支"
            ),
            data={
                "availableBudget": total_remaining,
                "requiredBranches": branch_count,
            },
        )

    distributable = total_remaining - branch_count

    if strategy == "weighted" and weights is not None:
        if len(weights) != branch_count:
            raise ValueError(
                f"权重列表长度 {len(weights)} 与分支数 {branch_count} 不一致"
            )
        total_weight = sum(weights)
        if total_weight == 0:
            return [0] * branch_count

        # 按权重分配（整数向下取整，余量分配给权重最大的分支）
        allocated = [int(distributable * w / total_weight) for w in weights]
        remainder = distributable - sum(allocated)
        # 将余量按权重从大到小依次分配
        sorted_indices = sorted(
            range(branch_count), key=lambda i: weights[i], reverse=True
        )
        for i in range(remainder):
            allocated[sorted_indices[i % branch_count]] += 1
        return allocated
    else:
        # 均分策略
        base = distributable // branch_count
        remainder = distributable % branch_count
        allocated = [base] * branch_count
        # 余量从最后一个分支开始分配
        for i in range(remainder):
            allocated[-(i + 1)] += 1
        return allocated
