"""ACS 模型兼容层测试。"""

from acps_sdk.acs.models import MutualTLSSecurityScheme


def test_mutual_tls_security_scheme_allows_missing_legacy_challenge_url() -> None:
    """主链路测试：EAB 模式下 MutualTLSSecurityScheme 不再要求 legacy challenge URL。"""
    scheme = MutualTLSSecurityScheme(type="mutualTLS")

    assert scheme.type == "mutualTLS"
    assert scheme.x_ca_challenge_base_url is None
