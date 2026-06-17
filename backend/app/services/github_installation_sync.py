import datetime
import logging
import time
from typing import Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models.account import Organization
from app.db.models.installation import Installation
from app.services.github_service import GitHubService

logger = logging.getLogger(__name__)


def _int_or_none(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _account_type(account: dict) -> str:
    return account.get("type") or "Organization"


def _account_name(account: dict) -> Optional[str]:
    return account.get("name") or account.get("login")


def _fallback_installed_at(installation_data: dict) -> str:
    return (
        installation_data.get("created_at")
        or installation_data.get("updated_at")
        or datetime.datetime.utcnow().isoformat()
    )


def upsert_installation_record(
    db: Session,
    installation_data: dict,
    account_data: dict,
    sender: Optional[dict] = None,
) -> Installation:
    installation_id = _int_or_none(installation_data.get("id"))
    account_id = _int_or_none(account_data.get("id"))

    if installation_id is None:
        raise ValueError(f"GitHub installation payload is missing id: {installation_data}")
    if account_id is None:
        raise ValueError(f"GitHub account payload is missing id: {account_data}")

    account_type = _account_type(account_data)
    organization = (
        db.query(Organization).filter(Organization.github_id == account_id).first()
    )

    if organization:
        organization.login = account_data["login"]
        organization.name = _account_name(account_data)
        organization.avatar_url = account_data.get("avatar_url")
        organization.type = account_type
        organization.updated_at = datetime.datetime.utcnow()
    else:
        organization = Organization(
            id=f"org_{account_type.lower()}_{account_id}_{int(time.time())}",
            github_id=account_id,
            login=account_data["login"],
            name=_account_name(account_data),
            avatar_url=account_data.get("avatar_url"),
            type=account_type,
        )
        db.add(organization)
        db.flush()

    installation = (
        db.query(Installation)
        .filter(Installation.installation_id == installation_id)
        .first()
    )

    app_id = _int_or_none(installation_data.get("app_id")) or _int_or_none(
        settings.GITHUB_APP_ID
    )
    sender_id = _int_or_none((sender or {}).get("id"))

    if installation:
        installation.organization_id = organization.id
        installation.installer_github_id = (
            installation.installer_github_id or sender_id
        )
        installation.app_id = installation.app_id or app_id
        installation.active = True
        installation.updated_at = datetime.datetime.utcnow()
    else:
        installation = Installation(
            installation_id=installation_id,
            organization_id=organization.id,
            installer_github_id=sender_id,
            app_id=app_id,
            installed_at=_fallback_installed_at(installation_data),
            active=True,
        )
        db.add(installation)

    db.commit()
    db.refresh(installation)
    logger.info(
        "Upserted GitHub App installation %s for %s",
        installation.installation_id,
        organization.login,
    )
    return installation


def ensure_installation_from_webhook_payload(
    db: Session, payload: dict
) -> Optional[Installation]:
    installation_data = payload.get("installation")
    if not installation_data:
        return None

    account_data = (
        payload.get("repository", {}).get("owner")
        or installation_data.get("account")
    )
    if not account_data:
        logger.warning(
            "Webhook payload for installation %s has no account/owner data",
            installation_data.get("id"),
        )
        return None

    return upsert_installation_record(
        db=db,
        installation_data=installation_data,
        account_data=account_data,
        sender=payload.get("sender"),
    )


async def sync_github_app_installations(db: Session) -> int:
    service = GitHubService(db)
    installations = await service.get_app_installations()
    synced = 0

    for installation in installations:
        account = installation.get("account")
        if not account:
            logger.warning("Skipping installation without account: %s", installation)
            continue

        upsert_installation_record(
            db=db,
            installation_data=installation,
            account_data=account,
        )
        synced += 1

    return synced
