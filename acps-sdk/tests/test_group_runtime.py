from acps_sdk.aip.aip_group_runtime import (build_group_exchange_name,
                                            build_group_queue_name,
                                            build_inbox_queue_name,
                                            normalize_group_id,
                                            parse_amqp_endpoint_url)

LEADER_AIC = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
PARTNER_AIC = "1.2.156.3088.1.1.34C2.478BDF.3GF547.0JUE"


def test_group_name_builders_follow_v21_convention() -> None:
    exchange = build_group_exchange_name(LEADER_AIC, "group-session-1")
    queue = build_group_queue_name(LEADER_AIC, "group-session-1", PARTNER_AIC)

    assert exchange == f"group_{LEADER_AIC}_group-session-1"
    assert queue == f"group_{LEADER_AIC}_group-session-1_{PARTNER_AIC}"
    assert build_inbox_queue_name(PARTNER_AIC) == f"inbox_{PARTNER_AIC}"


def test_parse_amqp_endpoint_url_replaces_aic_placeholder() -> None:
    endpoint = parse_amqp_endpoint_url(
        "amqps://mq.acps.example.com:5671/acps?inbox=inbox_{AIC}",
        aic=PARTNER_AIC,
    )

    assert endpoint.host == "mq.acps.example.com"
    assert endpoint.port == 5671
    assert endpoint.vhost == "acps"
    assert endpoint.inbox == f"inbox_{PARTNER_AIC}"


def test_normalize_group_id_replaces_invalid_session_separators() -> None:
    assert (
        normalize_group_id("group-sess_b50a1a6a8fb24024")
        == "group-sess-b50a1a6a8fb24024"
    )
