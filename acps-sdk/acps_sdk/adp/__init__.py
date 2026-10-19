"""
ADP (Agent Discovery Protocol) 智能体发现协议

本模块实现了 ACPs 协议体系中 ADP 协议的共用功能，
包括数据模型定义、错误码、验证工具和常量。

使用示例：

    # 构建发现请求
    from acps_sdk.adp import DiscoveryRequest, DiscoveryFilter, FilterCondition, FilterOperator

    request = DiscoveryRequest(
        type="explicit",
        query="我需要一个可以做北京美食推荐的智能体",
        limit=5,
        filter=DiscoveryFilter(
            conditions=[
                FilterCondition(field="active", op=FilterOperator.EQ, value=True),
                FilterCondition(field="skills.tags", op=FilterOperator.ANY_OF, value=["美食", "北京"]),
            ]
        ),
    )
    print(request.to_json())

    # 构建发现响应
    from acps_sdk.adp import (
        DiscoveryResponse, DiscoveryResult, DiscoveryRoute,
        DiscoveryAgentGroup, DiscoveryAgentSkill,
    )

    response = DiscoveryResponse.success(
        result=DiscoveryResult(
            acs_map={
                "AIC-001": {"name": "FoodAgent", "description": "美食推荐智能体"},
            },
            agents=[
                DiscoveryAgentGroup(
                    group="美食推荐",
                    agent_skills=[
                        DiscoveryAgentSkill(aic="AIC-001", skill_id="food-rec", ranking=1),
                    ],
                )
            ],
            routes=[
                DiscoveryRoute(
                    forward_chain=["AIC-DS-A"],
                    agent_groups=[
                        DiscoveryAgentGroup(
                            group="美食推荐",
                            agent_skills=[
                                DiscoveryAgentSkill(aic="AIC-001", skill_id="food-rec", ranking=1),
                            ],
                        )
                    ],
                    status="ok",
                    duration_ms=150,
                )
            ],
        )
    )

    # 验证转发链
    from acps_sdk.adp import validate_forward_chain
    validate_forward_chain(request, current_server_aic="AIC-DS-B", sender_aic="AIC-DS-A")

    # 构建转发请求
    from acps_sdk.adp import build_forwarded_request
    forwarded = build_forwarded_request(
        original=request,
        current_server_aic="AIC-DS-A",
        fanout_remaining_for_branch=1,
    )

    # 错误处理
    from acps_sdk.adp import ADPError, ADPErrorCode, make_error_response
    error_resp = make_error_response(ADPErrorCode.MISSING_QUERY)

    # 遍历发现结果中的智能体技能（自动关联 acsMap）
    for aic, acs_data, skill, group in response.result.iter_agent_skills():
        print(f"[{group}] {aic} -> skill={skill.skill_id}, acs={acs_data.get('name')}")

    # 错误分类（重定向 / 可重试 / 客户端错误 / 转发错误）
    if ADPErrorCode.CAPACITY_REDIRECT.is_redirect():
        print("需要重定向到其他发现服务器")
    if ADPErrorCode.CALLER_RATE_LIMITED.is_retryable():
        print("限流错误，可稍后重试")

    # 从错误响应中提取 ADPError
    error_response = DiscoveryResponse.failure(40001, "MissingQuery")
    adp_error = error_response.get_adp_error()
    if adp_error and adp_error.is_client_error():
        print(f"客户端参数错误: {adp_error.message}")
"""

from .models import (
    # 核心数据模型
    DiscoveryRequest,
    DiscoveryResponse,
    DiscoveryResult,
    DiscoveryRoute,
    DiscoveryAgentGroup,
    DiscoveryAgentSkill,
    # 过滤相关
    DiscoveryFilter,
    DiscoveryContext,
    FilterCondition,
    FilterOperator,
    # 错误信息
    ErrorDetail,
)

from .errors import (
    # 错误码枚举
    ADPErrorCode,
    # 异常类
    ADPError,
    # 错误码映射
    ADP_ERROR_NAMES,
    ADP_ERROR_HTTP_STATUS,
    # 便捷函数
    make_error_response,
    get_http_status_for_error,
)

from .constants import (
    # 转发深度
    FORWARD_DEPTH_LIMIT_DEFAULT,
    FORWARD_DEPTH_LIMIT_MAX,
    FORWARD_DEPTH_LIMIT_MIN,
    # 转发扇出
    FORWARD_FANOUT_LIMIT_DEFAULT,
    FORWARD_FANOUT_LIMIT_MAX,
    FORWARD_FANOUT_LIMIT_MIN,
    # 超时
    FORWARD_EACH_TIMEOUT_MS_DEFAULT,
    FORWARD_TOTAL_TIMEOUT_MS_DEFAULT,
    # 重定向
    MAX_REDIRECT_HOPS,
    # 过滤
    FILTER_MAX_NESTING_DEPTH,
    # 上下文
    CONTEXT_MAX_PAYLOAD_BYTES,
    # API 路径
    DISCOVER_API_PATH,
    # 请求头
    HEADER_TRACE_ID,
    HEADER_SPAN_ID,
    HEADER_PARENT_SPAN_ID,
    # 查询类型
    QUERY_TYPE_EXPLICIT,
    QUERY_TYPE_EXPLORATORY,
    QUERY_TYPE_TRENDING,
    QUERY_TYPE_FILTERED,
    QUERY_TYPES,
    # 路由状态
    ROUTE_STATUS_OK,
    ROUTE_STATUS_TIMEOUT,
    ROUTE_STATUS_ERROR,
    # 过滤逻辑
    FILTER_LOGIC_AND,
    FILTER_LOGIC_OR,
    FILTER_LOGIC_NOT,
)

from .validators import (
    # 请求验证
    validate_discovery_request,
    # 转发链验证
    validate_forward_chain,
    # 扇出额度验证
    validate_fanout_budget,
    # 信任列表验证
    validate_trusted_target,
    # 超时检查
    should_continue_forwarding,
    # 转发请求构建
    build_forwarded_request,
    allocate_fanout_budget,
)

__all__ = [
    # ── 数据模型 ──
    "DiscoveryRequest",
    "DiscoveryResponse",
    "DiscoveryResult",
    "DiscoveryRoute",
    "DiscoveryAgentGroup",
    "DiscoveryAgentSkill",
    "DiscoveryFilter",
    "DiscoveryContext",
    "FilterCondition",
    "FilterOperator",
    "ErrorDetail",
    # ── 错误处理 ──
    "ADPErrorCode",
    "ADPError",
    "ADP_ERROR_NAMES",
    "ADP_ERROR_HTTP_STATUS",
    "make_error_response",
    "get_http_status_for_error",
    # ── 常量 ──
    "FORWARD_DEPTH_LIMIT_DEFAULT",
    "FORWARD_DEPTH_LIMIT_MAX",
    "FORWARD_DEPTH_LIMIT_MIN",
    "FORWARD_FANOUT_LIMIT_DEFAULT",
    "FORWARD_FANOUT_LIMIT_MAX",
    "FORWARD_FANOUT_LIMIT_MIN",
    "FORWARD_EACH_TIMEOUT_MS_DEFAULT",
    "FORWARD_TOTAL_TIMEOUT_MS_DEFAULT",
    "MAX_REDIRECT_HOPS",
    "FILTER_MAX_NESTING_DEPTH",
    "CONTEXT_MAX_PAYLOAD_BYTES",
    "DISCOVER_API_PATH",
    "HEADER_TRACE_ID",
    "HEADER_SPAN_ID",
    "HEADER_PARENT_SPAN_ID",
    "QUERY_TYPE_EXPLICIT",
    "QUERY_TYPE_EXPLORATORY",
    "QUERY_TYPE_TRENDING",
    "QUERY_TYPE_FILTERED",
    "QUERY_TYPES",
    "ROUTE_STATUS_OK",
    "ROUTE_STATUS_TIMEOUT",
    "ROUTE_STATUS_ERROR",
    "FILTER_LOGIC_AND",
    "FILTER_LOGIC_OR",
    "FILTER_LOGIC_NOT",
    # ── 验证工具 ──
    "validate_discovery_request",
    "validate_forward_chain",
    "validate_fanout_budget",
    "validate_trusted_target",
    "should_continue_forwarding",
    "build_forwarded_request",
    "allocate_fanout_budget",
]
