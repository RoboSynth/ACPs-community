from __future__ import annotations

import logging
import ssl
from typing import Optional
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)


class GroupAclError(RuntimeError):
    pass


class GroupAclClient:
    def __init__(
        self,
        *,
        base_url: str,
        ssl_context: Optional[ssl.SSLContext] = None,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            verify=ssl_context if ssl_context is not None else True,
            timeout=timeout,
        )

    async def add_member(
        self, *, leader_aic: str, group_id: str, member_aic: str
    ) -> None:
        await self._request(
            "PUT",
            f"/groups/{quote(leader_aic, safe='')}/{quote(group_id, safe='')}"
            f"/members/{quote(member_aic, safe='')}",
        )

    async def remove_member(
        self, *, leader_aic: str, group_id: str, member_aic: str
    ) -> None:
        await self._request(
            "DELETE",
            f"/groups/{quote(leader_aic, safe='')}/{quote(group_id, safe='')}"
            f"/members/{quote(member_aic, safe='')}",
        )

    async def delete_group(self, *, leader_aic: str, group_id: str) -> None:
        await self._request(
            "DELETE",
            f"/groups/{quote(leader_aic, safe='')}/{quote(group_id, safe='')}",
        )

    async def close_member_connection(
        self, *, leader_aic: str, group_id: str, member_aic: str
    ) -> None:
        await self._request(
            "DELETE",
            f"/groups/{quote(leader_aic, safe='')}/{quote(group_id, safe='')}"
            f"/members/{quote(member_aic, safe='')}/connection",
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str) -> None:
        logger.info(
            "event=group_acl_client_request method=%s base_url=%s path=%s",
            method,
            self.base_url,
            path,
        )
        response = await self._client.request(method, path)
        if response.status_code >= 400:
            detail = response.text.strip() or response.reason_phrase
            logger.error(
                "event=group_acl_client_request_failed method=%s path=%s status=%s detail=%s",
                method,
                path,
                response.status_code,
                detail,
            )
            raise GroupAclError(
                f"{method} {path} failed: {response.status_code} {detail}"
            )
        logger.info(
            "event=group_acl_client_request_done method=%s path=%s status=%s",
            method,
            path,
            response.status_code,
        )
