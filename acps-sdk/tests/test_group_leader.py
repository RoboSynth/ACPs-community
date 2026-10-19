import asyncio
from unittest.mock import AsyncMock, MagicMock

from aio_pika.exceptions import DeliveryError

from acps_sdk.aip.aip_group_leader import (
    GroupLeader,
    GroupLeaderMqClient,
    GroupLeaderSession,
    PartnerConnectionInfo,
)
from acps_sdk.aip.aip_group_model import (
    GroupMemberStatus,
    GroupMgmtCommandType,
    GroupMgmtResult,
)

VALID_LEADER_AIC = "1.2.156.3088.1.1.CIQJUQ.HELDGD.1.03TO"
VALID_PARTNER_AIC = "1.2.156.3088.1.1.8UDX9U.NNVB61.1.13WT"


def test_send_mgmt_command_with_explicit_session_id() -> None:
    """管理命令应使用显式传入的 session_id，而非从 groupId 推导"""
    client = GroupLeaderMqClient(leader_aic="leader-aic")
    client._group_id = "group-session-123"

    published = {}

    async def fake_publish(message):
        published["message"] = message

    client.publish_message = fake_publish  # type: ignore[method-assign]

    message = asyncio.run(
        client.send_mgmt_command(
            GroupMgmtCommandType.GET_STATUS,
            mentions="all",
            session_id="session-91011",
        )
    )

    assert message.groupId == "group-session-123"
    assert message.sessionId == "session-91011"
    assert published["message"].sessionId == "session-91011"


def test_send_mgmt_command_without_session_id() -> None:
    """不传 session_id 时，sessionId 应为 None"""
    client = GroupLeaderMqClient(leader_aic="leader-aic")
    client._group_id = "group123"

    published = {}

    async def fake_publish(message):
        published["message"] = message

    client.publish_message = fake_publish  # type: ignore[method-assign]

    message = asyncio.run(
        client.send_mgmt_command(GroupMgmtCommandType.GET_STATUS, mentions="all")
    )

    assert message.groupId == "group123"
    assert message.sessionId is None
    assert published["message"].sessionId is None


def test_request_partner_leave_retries_on_delivery_error() -> None:
    client = GroupLeaderMqClient(leader_aic="leader-aic")
    client._group_id = "group123"

    expected = MagicMock()
    client.send_mgmt_command = AsyncMock(  # type: ignore[method-assign]
        side_effect=[DeliveryError(None, MagicMock()), expected]
    )

    result = asyncio.run(
        client.request_partner_leave("partner-aic", session_id="session-1")
    )

    assert result is expected
    assert client.send_mgmt_command.await_count == 2


def test_request_partner_leave_becomes_noop_when_partner_already_disconnected() -> None:
    client = GroupLeaderMqClient(leader_aic="leader-aic")
    client._group_id = "group123"
    client._partners["partner-aic"] = PartnerConnectionInfo(
        aic="partner-aic",
        connection_name="conn-1",
        vhost="acps",
        node_name="rabbit@node",
        queue_name="group_leader-aic_group123_partner-aic",
    )
    client._partners["partner-aic"].connected = False
    client.send_mgmt_command = AsyncMock()  # type: ignore[method-assign]

    result = asyncio.run(
        client.request_partner_leave("partner-aic", session_id="session-1")
    )

    client.send_mgmt_command.assert_not_awaited()
    assert result.command == GroupMgmtCommandType.LEAVE_GROUP
    assert result.mentions == ["partner-aic"]
    assert result.sessionId == "session-1"


def test_request_partner_leave_becomes_noop_when_partner_disconnects_after_delivery_error() -> (
    None
):
    client = GroupLeaderMqClient(leader_aic="leader-aic")
    client._group_id = "group123"
    partner = PartnerConnectionInfo(
        aic="partner-aic",
        connection_name="conn-1",
        vhost="acps",
        node_name="rabbit@node",
        queue_name="group_leader-aic_group123_partner-aic",
    )
    client._partners["partner-aic"] = partner

    async def fake_send_mgmt_command(*args, **kwargs):  # type: ignore[no-untyped-def]
        partner.connected = False
        raise DeliveryError(None, MagicMock())

    client.send_mgmt_command = fake_send_mgmt_command  # type: ignore[method-assign]

    result = asyncio.run(
        client.request_partner_leave("partner-aic", session_id="session-1")
    )

    assert result.command == GroupMgmtCommandType.LEAVE_GROUP
    assert result.mentions == ["partner-aic"]
    assert result.sessionId == "session-1"


def test_delete_partner_queue_uses_channel_queue_delete() -> None:
    client = GroupLeaderMqClient(leader_aic=VALID_LEADER_AIC)
    client._group_id = "group123"

    channel = MagicMock()
    channel.queue_delete = AsyncMock()
    client._channel = channel  # type: ignore[assignment]

    result = asyncio.run(client.delete_partner_queue(VALID_PARTNER_AIC))

    channel.queue_delete.assert_awaited_once_with(
        "group_1.2.156.3088.1.1.CIQJUQ.HELDGD.1.03TO_group123_1.2.156.3088.1.1.8UDX9U.NNVB61.1.13WT",
        if_unused=False,
        if_empty=False,
    )
    assert result is True


def test_cleanup_group_resources_uses_channel_delete_and_clears_refs() -> None:
    client = GroupLeaderMqClient(leader_aic="leader-aic")
    client._group_id = "group123"
    client._exchange_name = "exchange-1"

    queue = MagicMock()
    queue.name = "queue-1"
    client._leader_queue = queue  # type: ignore[assignment]

    exchange = MagicMock()
    exchange.delete = AsyncMock()
    client._exchange = exchange  # type: ignore[assignment]

    channel = MagicMock()
    channel.queue_delete = AsyncMock()
    channel.exchange_delete = AsyncMock()
    client._channel = channel  # type: ignore[assignment]

    asyncio.run(client.cleanup_group_resources())

    channel.queue_delete.assert_awaited_once_with(
        "queue-1",
        if_unused=False,
        if_empty=False,
    )
    channel.exchange_delete.assert_awaited_once_with(
        "exchange-1",
        if_unused=False,
    )
    exchange.delete.assert_not_awaited()
    assert client._leader_queue is None
    assert client._exchange is None
    assert client._exchange_name is None


def test_group_runtime_exposes_invitation_route() -> None:
    leader = GroupLeader(
        "leader-aic",
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
    )
    session = GroupLeaderSession(
        "session-1",
        "group-session-1",
        GroupLeaderMqClient(leader_aic="leader-aic"),
    )
    session.record_invitation_route("partner-aic", "inbox")
    session.leader_mq_client._partners["partner-aic"] = PartnerConnectionInfo(
        aic="partner-aic",
        connection_name="conn-1",
        vhost="acps",
        node_name="rabbit@node",
        queue_name="group_leader-aic_group-session-1_partner-aic",
    )
    leader.group_sessions["session-1"] = session

    runtime = leader.get_group_runtime("session-1")

    assert runtime["group_id"] == "group-session-1"
    assert runtime["connected_members"] == 1
    assert runtime["members"][0]["partner_aic"] == "partner-aic"
    assert runtime["members"][0]["invitation_route"] == "inbox"


def test_force_remove_partner_cleans_acl_queue_and_runtime() -> None:
    leader = GroupLeader(
        "leader-aic",
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
    )
    acl_client = MagicMock()
    acl_client.remove_member = AsyncMock()
    acl_client.close_member_connection = AsyncMock()
    leader._group_acl_client = acl_client
    leader._wait_for_partner_disconnect = AsyncMock(return_value=False)  # type: ignore[method-assign]

    mq_client = GroupLeaderMqClient(leader_aic="leader-aic")
    call_order: list[str] = []

    async def fake_request_partner_leave(*args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        call_order.append("leave")

    async def fake_delete_partner_queue(partner_aic: str) -> bool:
        call_order.append(f"delete:{partner_aic}")
        return True

    async def fake_remove_member(**kwargs) -> None:  # type: ignore[no-untyped-def]
        call_order.append("remove")

    async def fake_close_member_connection(**kwargs) -> None:  # type: ignore[no-untyped-def]
        call_order.append("close")

    def fake_remove_partner_tracking(partner_aic: str) -> None:
        call_order.append(f"track:{partner_aic}")

    mq_client.delete_partner_queue = AsyncMock(side_effect=fake_delete_partner_queue)  # type: ignore[method-assign]
    mq_client.request_partner_leave = AsyncMock(side_effect=fake_request_partner_leave)  # type: ignore[method-assign]
    mq_client.remove_partner_tracking = MagicMock(side_effect=fake_remove_partner_tracking)  # type: ignore[method-assign]
    acl_client.remove_member = AsyncMock(side_effect=fake_remove_member)
    acl_client.close_member_connection = AsyncMock(
        side_effect=fake_close_member_connection
    )

    session = GroupLeaderSession("session-1", "group-session-1", mq_client)
    leader.group_sessions["session-1"] = session

    result = asyncio.run(
        leader.force_remove_partner("partner-aic", session_id="session-1")
    )

    acl_client.remove_member.assert_awaited_once_with(
        leader_aic="leader-aic",
        group_id="group-session-1",
        member_aic="partner-aic",
    )
    mq_client.delete_partner_queue.assert_awaited_once_with("partner-aic")
    acl_client.close_member_connection.assert_awaited_once_with(
        leader_aic="leader-aic",
        group_id="group-session-1",
        member_aic="partner-aic",
    )
    mq_client.request_partner_leave.assert_awaited_once_with(
        "partner-aic",
        session_id="session-1",
    )
    mq_client.remove_partner_tracking.assert_called_once_with("partner-aic")
    assert call_order == [
        "leave",
        "remove",
        "delete:partner-aic",
        "close",
        "track:partner-aic",
    ]
    assert result == {
        "session_id": "session-1",
        "group_id": "group-session-1",
        "partner_aic": "partner-aic",
        "queue_deleted": True,
    }


def test_force_remove_partner_prefers_graceful_leave_cleanup() -> None:
    leader = GroupLeader(
        "leader-aic",
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
    )
    acl_client = MagicMock()
    acl_client.remove_member = AsyncMock()
    acl_client.close_member_connection = AsyncMock()
    leader._group_acl_client = acl_client
    leader._wait_for_partner_disconnect = AsyncMock(return_value=True)  # type: ignore[method-assign]

    mq_client = GroupLeaderMqClient(leader_aic="leader-aic")
    mq_client.delete_partner_queue = AsyncMock(return_value=True)  # type: ignore[method-assign]
    mq_client.request_partner_leave = AsyncMock()  # type: ignore[method-assign]
    mq_client.remove_partner_tracking = MagicMock()  # type: ignore[method-assign]

    session = GroupLeaderSession("session-1", "group-session-1", mq_client)
    leader.group_sessions["session-1"] = session

    result = asyncio.run(
        leader.force_remove_partner("partner-aic", session_id="session-1")
    )

    mq_client.request_partner_leave.assert_awaited_once_with(
        "partner-aic",
        session_id="session-1",
    )
    leader._wait_for_partner_disconnect.assert_awaited_once_with(  # type: ignore[attr-defined]
        session,
        "partner-aic",
    )
    acl_client.remove_member.assert_awaited_once_with(
        leader_aic="leader-aic",
        group_id="group-session-1",
        member_aic="partner-aic",
    )
    mq_client.delete_partner_queue.assert_not_awaited()
    acl_client.close_member_connection.assert_not_awaited()
    mq_client.remove_partner_tracking.assert_called_once_with("partner-aic")
    assert result == {
        "session_id": "session-1",
        "group_id": "group-session-1",
        "partner_aic": "partner-aic",
        "queue_deleted": False,
    }


def test_group_mgmt_result_disconnected_cleans_acl() -> None:
    leader = GroupLeader(
        "leader-aic",
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
    )
    acl_client = MagicMock()
    acl_client.remove_member = AsyncMock()
    leader._group_acl_client = acl_client

    session = GroupLeaderSession(
        "session-1",
        "group-session-1",
        GroupLeaderMqClient(leader_aic="leader-aic"),
    )
    leader.group_sessions["session-1"] = session

    message = GroupMgmtResult(
        id="mgmt-result-1",
        sentAt="2026-04-15T00:00:00+00:00",
        senderRole="partner",
        senderId="partner-aic",
        dataItems=[],
        status=GroupMemberStatus(connected=False, muted=False),
        groupId="group-session-1",
        sessionId="session-1",
    )

    asyncio.run(leader._handle_group_message(message))

    acl_client.remove_member.assert_awaited_once_with(
        leader_aic="leader-aic",
        group_id="group-session-1",
        member_aic="partner-aic",
    )
