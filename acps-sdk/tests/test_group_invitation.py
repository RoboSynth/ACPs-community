import asyncio
import ssl
from unittest.mock import AsyncMock, Mock

import pytest

from acps_sdk.aip.aip_base_model import TaskState
from acps_sdk.aip.aip_group_leader import (GroupLeader, GroupLeaderMqClient,
                                           GroupLeaderSession,
                                           PartnerInviteError,
                                           PendingInvitation)
from acps_sdk.aip.aip_group_model import (ACSObject, AMQPConfig, GroupInfo,
                                          GroupInvitationError,
                                          GroupInvitationErrorData,
                                          InboxGroupInvitation,
                                          InboxGroupInvitationError,
                                          RabbitMQRequest,
                                          RabbitMQRequestParams,
                                          RabbitMQServerConfig)
from acps_sdk.aip.aip_group_partner import (GroupPartnerMqClient,
                                            PartnerGroupState)
from acps_sdk.aip.aip_group_runtime import calculate_invitation_expiry

LEADER_AIC = "1.2.156.3088.1.1.34C2.478BDF.3GF546.0JU4"
PARTNER_AIC = "1.2.156.3088.1.1.34C2.478BDF.3GF547.0JUE"


def _build_group_info() -> GroupInfo:
    return GroupInfo(
        groupId="group-session-1",
        leader=ACSObject(aic=LEADER_AIC),
        partners=[ACSObject(aic=PARTNER_AIC)],
    )


def _build_amqp_config() -> AMQPConfig:
    return AMQPConfig(
        exchange=f"group_{LEADER_AIC}_group-session-1",
        exchangeType="fanout",
        routingKey="",
    )


def test_group_leader_prefers_amqp_endpoint() -> None:
    leader = GroupLeader(
        LEADER_AIC,
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
    )
    session = GroupLeaderSession(
        "session-1",
        "group-session-1",
        GroupLeaderMqClient(leader_aic=LEADER_AIC),
    )
    leader.group_sessions["session-1"] = session
    called: dict[str, str] = {}

    async def fake_inbox(*, session, partner_acs, partner_inbox) -> None:
        called["route"] = "inbox"
        called["inbox"] = partner_inbox

    async def fake_rpc(*, session, partner_acs, rpc_url) -> None:
        called["route"] = "rpc"

    leader._invite_partner_via_inbox = fake_inbox  # type: ignore[method-assign]
    leader._invite_partner_via_rpc = fake_rpc  # type: ignore[method-assign]

    asyncio.run(
        leader.invite_partner(
            session_id="session-1",
            partner_acs=ACSObject(aic=PARTNER_AIC),
            partner_acs_data={
                "endPoints": [
                    {
                        "transport": "AMQP",
                        "url": "amqps://mq.acps.example.com:5671/acps?inbox=inbox_{AIC}",
                    },
                    {
                        "transport": "JSONRPC",
                        "url": "https://partner.example.com/group/rpc",
                    },
                ]
            },
        )
    )

    assert called == {"route": "inbox", "inbox": f"inbox_{PARTNER_AIC}"}


def test_group_leader_falls_back_to_rpc_when_amqp_server_mismatch() -> None:
    leader = GroupLeader(
        LEADER_AIC,
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
    )
    session = GroupLeaderSession(
        "session-1",
        "group-session-1",
        GroupLeaderMqClient(leader_aic=LEADER_AIC),
    )
    leader.group_sessions["session-1"] = session
    called: dict[str, str] = {}

    async def fake_inbox(*, session, partner_acs, partner_inbox) -> None:
        called["route"] = "inbox"

    async def fake_rpc(*, session, partner_acs, rpc_url) -> None:
        called["route"] = "rpc"
        called["rpc_url"] = rpc_url

    leader._invite_partner_via_inbox = fake_inbox  # type: ignore[method-assign]
    leader._invite_partner_via_rpc = fake_rpc  # type: ignore[method-assign]

    asyncio.run(
        leader.invite_partner(
            session_id="session-1",
            partner_acs=ACSObject(aic=PARTNER_AIC),
            partner_acs_data={
                "endPoints": [
                    {
                        "transport": "AMQP",
                        "url": "amqps://other-mq.example.com:5671/acps?inbox=inbox_{AIC}",
                    },
                    {
                        "transport": "JSONRPC",
                        "url": "https://partner.example.com/group/rpc",
                    },
                ]
            },
        )
    )

    assert called == {
        "route": "rpc",
        "rpc_url": "https://partner.example.com/group/rpc",
    }


def test_partner_direct_rpc_rejection_uses_invitation_error_code() -> None:
    client = GroupPartnerMqClient(partner_aic=PARTNER_AIC)

    async def reject(_invitation: InboxGroupInvitation) -> bool:
        return False

    client.set_invitation_handler(reject)

    request = RabbitMQRequest(
        id="req-1",
        params=RabbitMQRequestParams(
            protocol="rabbitmq:4.2",
            group=_build_group_info(),
            server=RabbitMQServerConfig(
                host="mq.acps.example.com",
                port=5671,
                vhost="acps",
            ),
            amqp=_build_amqp_config(),
        ),
    )

    response = asyncio.run(client.join_group(request))

    assert response.error is not None
    assert response.error.code == -32020
    assert response.error.data is not None
    assert response.error.data.errorType == "INVITATION_REJECTED"


def test_partner_invitation_handler_rejection_sends_inbox_error() -> None:
    client = GroupPartnerMqClient(
        partner_aic=PARTNER_AIC,
        ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
    )

    async def reject(_invitation: InboxGroupInvitation) -> bool:
        return False

    sent: dict[str, object] = {}

    async def fake_send_invitation_error(
        invitation: InboxGroupInvitation, error
    ) -> None:
        sent["invitation"] = invitation
        sent["error"] = error

    client.set_invitation_handler(reject)
    client.send_invitation_error = fake_send_invitation_error  # type: ignore[method-assign]

    invitation = InboxGroupInvitation(
        protocol="rabbitmq:4.2",
        expiresAt=calculate_invitation_expiry(300),
        invitationToken="token-1",
        group=_build_group_info(),
        amqp=_build_amqp_config(),
    )

    joined = asyncio.run(client.join_group_from_invitation(invitation))

    assert joined is False
    assert sent["invitation"] == invitation
    error = sent["error"]
    assert getattr(error, "code") == -32020
    assert getattr(error, "data").errorType == "INVITATION_REJECTED"


def test_partner_connect_uses_robust_connection_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GroupPartnerMqClient(partner_aic=PARTNER_AIC)
    fake_connection = AsyncMock()
    fake_channel = AsyncMock()
    fake_connection.channel = AsyncMock(return_value=fake_channel)
    connect = AsyncMock(return_value=fake_connection)
    connect_robust = AsyncMock(return_value=fake_connection)

    monkeypatch.setattr("acps_sdk.aip.aip_group_partner.aio_pika.connect", connect)
    monkeypatch.setattr(
        "acps_sdk.aip.aip_group_partner.aio_pika.connect_robust",
        connect_robust,
    )

    asyncio.run(client.connect())

    connect_robust.assert_awaited_once()
    connect.assert_not_called()
    assert client.connection is fake_connection
    assert client._channel is fake_channel


def test_partner_connect_can_disable_robust_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GroupPartnerMqClient(
        partner_aic=PARTNER_AIC,
        robust_connection=False,
    )
    fake_connection = AsyncMock()
    fake_channel = AsyncMock()
    fake_connection.channel = AsyncMock(return_value=fake_channel)
    connect = AsyncMock(return_value=fake_connection)
    connect_robust = AsyncMock(return_value=fake_connection)

    monkeypatch.setattr("acps_sdk.aip.aip_group_partner.aio_pika.connect", connect)
    monkeypatch.setattr(
        "acps_sdk.aip.aip_group_partner.aio_pika.connect_robust",
        connect_robust,
    )

    asyncio.run(client.connect())

    connect.assert_awaited_once()
    connect_robust.assert_not_called()
    assert client.connection is fake_connection
    assert client._channel is fake_channel


def test_partner_leave_group_announces_exit_before_cleanup() -> None:
    client = GroupPartnerMqClient(partner_aic=PARTNER_AIC)
    client._state = PartnerGroupState.JOINED
    client._group_id = "group-session-1"
    client._exchange = object()  # type: ignore[assignment]
    client._muted = False

    published: dict[str, object] = {}
    status_args: dict[str, object] = {}

    async def fake_publish(message) -> None:
        published["message"] = message

    async def fake_send_status_result(
        connected: bool,
        muted: bool,
        session_id: str | None = None,
    ) -> None:
        status_args["connected"] = connected
        status_args["muted"] = muted
        status_args["session_id"] = session_id

    client._publish_message = fake_publish  # type: ignore[method-assign]
    client.send_status_result = fake_send_status_result  # type: ignore[method-assign]
    client._cleanup_group_resources = AsyncMock()  # type: ignore[method-assign]

    asyncio.run(client.leave_group(session_id="session-1"))

    message = published["message"]
    assert message.senderRole == "partner"
    assert message.senderId == PARTNER_AIC
    assert message.command == "leave-group"
    assert message.mentions == [PARTNER_AIC]
    assert message.groupId == "group-session-1"
    assert message.sessionId == "session-1"
    assert status_args == {
        "connected": False,
        "muted": False,
        "session_id": "session-1",
    }
    client._cleanup_group_resources.assert_awaited_once()  # type: ignore[attr-defined]


def test_partner_cleanup_cancels_registered_consumer_before_queue_delete() -> None:
    client = GroupPartnerMqClient(partner_aic=PARTNER_AIC)
    client._state = PartnerGroupState.JOINED
    client._group_id = "group-session-1"

    queue = Mock()
    queue.name = "queue-1"
    queue.consume = AsyncMock(return_value="consumer-1")
    queue.cancel = AsyncMock()
    queue.delete = AsyncMock()
    client._queue = queue  # type: ignore[assignment]

    asyncio.run(client._start_consuming())

    assert client._consumer_tag == "consumer-1"
    queue.consume.assert_awaited_once()

    asyncio.run(client._cleanup_group_resources())

    queue.cancel.assert_awaited_once_with("consumer-1")
    queue.delete.assert_awaited_once_with(if_unused=False, if_empty=False)
    assert client._consumer_tag is None
    assert client.state == PartnerGroupState.DISCONNECTED


def test_partner_cleanup_resets_group_state_and_notifies_disconnect_handler() -> None:
    client = GroupPartnerMqClient(partner_aic=PARTNER_AIC)
    client._state = PartnerGroupState.JOINED
    client._group_id = "group-session-1"
    client._group_info = _build_group_info()
    client._exchange_name = "exchange-1"
    client._queue_name = "queue-1"
    client._muted = True
    client._pending_leave = True
    client._pending_leave_session_id = "session-1"
    client._task_states["task-1"] = TaskState.Working

    channel = Mock()
    channel.is_closed = False
    channel.close = AsyncMock()
    client._channel = channel  # type: ignore[assignment]

    connection = Mock()
    connection.is_closed = False
    connection.close = AsyncMock()
    client._connection = connection  # type: ignore[assignment]
    client._connection_owner = True

    disconnect_handler = AsyncMock()
    client.set_disconnect_handler(disconnect_handler)

    asyncio.run(client._cleanup_group_resources())

    channel.close.assert_awaited_once()
    connection.close.assert_awaited_once()
    disconnect_handler.assert_awaited_once()
    assert disconnect_handler.await_args.args == (client, "group-session-1")
    assert client.state == PartnerGroupState.DISCONNECTED
    assert client.group_id is None
    assert client.queue_name is None
    assert client.connection is None
    assert client._group_info is None
    assert client._exchange_name is None
    assert client._connection_name is None
    assert client._vhost is None
    assert client._node_name is None
    assert client._pending_leave is False
    assert client._pending_leave_session_id is None
    assert client.is_muted is False
    assert client._task_states == {}


def test_partner_handle_leave_request_continues_cleanup_when_status_publish_fails() -> (
    None
):
    client = GroupPartnerMqClient(partner_aic=PARTNER_AIC)
    client._state = PartnerGroupState.JOINED
    client._group_id = "group-session-1"
    client._pending_leave_session_id = "session-1"
    client._cleanup_group_resources = AsyncMock()  # type: ignore[method-assign]

    async def fake_send_status_result(
        connected: bool,
        muted: bool,
        session_id: str | None = None,
    ) -> None:
        del connected, muted, session_id
        raise RuntimeError("delivery nack")

    client.send_status_result = fake_send_status_result  # type: ignore[method-assign]

    asyncio.run(client._handle_leave_request())

    client._cleanup_group_resources.assert_awaited_once()  # type: ignore[attr-defined]
    assert client._pending_leave_session_id is None


def test_partner_access_token_four_state_logic() -> None:
    client = GroupPartnerMqClient(
        partner_aic=PARTNER_AIC,
        ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
    )

    client._prepare_connection_from_server(  # type: ignore[attr-defined]
        RabbitMQServerConfig(host="mq.acps.example.com", port=5671, vhost="acps")
    )
    assert client.rabbitmq_user is None
    assert client.rabbitmq_password is None

    client._prepare_connection_from_server(  # type: ignore[attr-defined]
        RabbitMQServerConfig(
            host="mq.acps.example.com",
            port=5671,
            vhost="acps",
            accessToken=None,
        )
    )
    assert client.rabbitmq_user is None
    assert client.rabbitmq_password is None

    client._prepare_connection_from_server(  # type: ignore[attr-defined]
        RabbitMQServerConfig(
            host="mq.acps.example.com",
            port=5672,
            vhost="/",
            accessToken="guest:guest",
        )
    )
    assert client.rabbitmq_user == "guest"
    assert client.rabbitmq_password == "guest"

    with pytest.raises(ValueError, match="Malformed accessToken"):
        client._prepare_connection_from_server(  # type: ignore[attr-defined]
            RabbitMQServerConfig(
                host="mq.acps.example.com",
                port=5671,
                vhost="acps",
                accessToken="",
            )
        )


def test_leader_invitation_timeout_cleans_acl_and_fails_pending_future() -> None:
    leader = GroupLeader(
        LEADER_AIC,
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
        invitation_timeout_seconds=0,
    )

    async def scenario() -> None:
        acl_client = AsyncMock()
        leader._group_acl_client = acl_client

        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        timeout_task = asyncio.create_task(asyncio.sleep(3600))
        leader._pending_invitations[PARTNER_AIC] = PendingInvitation(
            session_id="session-1",
            partner_aic=PARTNER_AIC,
            invitation_token="token-1",
            future=future,
            timeout_task=timeout_task,
        )

        await leader._watch_invitation_timeout(
            session_id="session-1",
            group_id="group-session-1",
            partner_aic=PARTNER_AIC,
            invitation_token="token-1",
        )

        acl_client.remove_member.assert_awaited_once_with(
            leader_aic=LEADER_AIC,
            group_id="group-session-1",
            member_aic=PARTNER_AIC,
        )
        assert future.done() is True
        assert isinstance(future.exception(), PartnerInviteError)
        assert leader._pending_invitations == {}
        with pytest.raises(asyncio.CancelledError):
            await timeout_task

    asyncio.run(scenario())


def test_leader_invitation_error_token_mismatch_preserves_pending_state() -> None:
    leader = GroupLeader(
        LEADER_AIC,
        {
            "host": "mq.acps.example.com",
            "port": 5671,
            "vhost": "acps",
        },
    )

    async def scenario() -> None:
        acl_client = AsyncMock()
        leader._group_acl_client = acl_client

        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        timeout_task = asyncio.create_task(asyncio.sleep(3600))
        leader._pending_invitations[PARTNER_AIC] = PendingInvitation(
            session_id="session-1",
            partner_aic=PARTNER_AIC,
            invitation_token="token-expected",
            future=future,
            timeout_task=timeout_task,
        )

        await leader._handle_invitation_error(
            InboxGroupInvitationError(
                groupId="group-session-1",
                partnerAic=PARTNER_AIC,
                invitationToken="token-actual",
                error=GroupInvitationError(
                    code=-32020,
                    message="rejected",
                    data=GroupInvitationErrorData(errorType="INVITATION_REJECTED"),
                ),
            )
        )

        acl_client.remove_member.assert_not_awaited()
        assert future.done() is False
        assert PARTNER_AIC in leader._pending_invitations

        timeout_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await timeout_task

    asyncio.run(scenario())
