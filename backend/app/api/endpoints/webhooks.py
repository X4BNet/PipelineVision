import asyncio
import hashlib
import hmac
import json
import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from app.api.endpoints.sse import broadcast_event
from app.core.config import settings
from app.db.models.installation import Installation
from app.db.session import get_db
from app.services.github_installation_sync import ensure_installation_from_webhook_payload
from app.services.github_service import GitHubService
from app.services.workflow_service import WorkflowService

router = APIRouter()
logger = logging.getLogger(__name__)


async def _verify_webhook_signature(
    request: Request, x_hub_signature_256: str = Header(None)
):
    if not x_hub_signature_256:
        raise HTTPException(
            status_code=401, detail="X-Hub-Signature-256 header is missing"
        )

    try:
        body = await request.body()
    except Exception:
        logger.exception("Failed to read webhook body")
        raise HTTPException(status_code=400, detail="Failed to read request body")

    mac = hmac.new(
        settings.GITHUB_APP_WEBHOOK_SECRET.encode(), msg=body, digestmod=hashlib.sha256
    )
    expected_signature = f"sha256={mac.hexdigest()}"

    if not hmac.compare_digest(expected_signature, x_hub_signature_256):
        raise HTTPException(status_code=401, detail="Invalid signature")

    return body


def _installation_record(db: Session, installation_id: int):
    return (
        db.query(Installation)
        .filter(Installation.installation_id == installation_id)
        .first()
    )


async def _broadcast_workflow_run(db: Session, installation_id: int, payload: dict):
    installation_record = _installation_record(db, installation_id)
    if not installation_record or not installation_record.organization_id:
        logger.debug("No installation record found for workflow_run SSE broadcast")
        return

    workflow_run = payload["workflow_run"]
    event_type = f"workflow_run_{payload.get('action', 'updated')}"
    event_data = {
        "run_id": str(workflow_run["id"]),
        "run_attempt": workflow_run.get("run_attempt", 1),
        "status": workflow_run.get("status"),
        "conclusion": workflow_run.get("conclusion"),
        "workflow_name": workflow_run.get("name"),
        "action": payload.get("action"),
    }

    asyncio.create_task(
        broadcast_event(
            installation_record.organization_id,
            event_type,
            event_data,
        )
    )


async def _broadcast_workflow_job(db: Session, installation_id: int, payload: dict):
    installation_record = _installation_record(db, installation_id)
    if not installation_record or not installation_record.organization_id:
        logger.debug("No installation record found for workflow_job SSE broadcast")
        return

    workflow_job = payload["workflow_job"]
    event_type = f"workflow_job_{payload.get('action', 'updated')}"
    event_data = {
        "job_id": str(workflow_job["id"]),
        "run_id": str(workflow_job.get("run_id")),
        "run_attempt": workflow_job.get("run_attempt", 1),
        "status": workflow_job.get("status"),
        "conclusion": workflow_job.get("conclusion"),
        "job_name": workflow_job.get("name"),
        "action": payload.get("action"),
    }

    asyncio.create_task(
        broadcast_event(
            installation_record.organization_id,
            event_type,
            event_data,
        )
    )


@router.post("/github")
async def github_webhook(
    body: bytes = Depends(_verify_webhook_signature),
    db: Session = Depends(get_db),
):
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.error("Invalid JSON payload")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    workflow_service = WorkflowService(db)

    if "workflow_run" in payload:
        installation = payload.get("installation")
        workflow_run = payload.get("workflow_run")
        repository = payload.get("repository")

        if not installation or not workflow_run or not repository:
            logger.warning(
                "Missing required fields in workflow_run payload: %s", payload
            )
            raise HTTPException(status_code=400, detail="Missing required fields")

        ensure_installation_from_webhook_payload(db, payload)

        logger.info(
            "Processing workflow_run event for installation %s, repository %s, run %s, action: %s",
            installation["id"],
            repository["full_name"],
            workflow_run["id"],
            payload.get("action", "unknown"),
        )

        await workflow_service.process_workflow_run_event(
            installation["id"], workflow_run, repository
        )

        try:
            await _broadcast_workflow_run(db, installation["id"], payload)
        except Exception as exc:
            logger.debug("SSE workflow_run broadcast failed: %s", exc)

        return {"status": "success", "message": "Processed workflow_run event"}

    if "workflow_job" in payload:
        installation = payload.get("installation")
        workflow_job = payload.get("workflow_job")
        repository = payload.get("repository")

        if not installation or not workflow_job or not repository:
            logger.warning(
                "Missing required fields in workflow_job payload: %s", payload
            )
            raise HTTPException(status_code=400, detail="Missing required fields")

        ensure_installation_from_webhook_payload(db, payload)

        logger.info(
            "Processing workflow_job event for installation %s, repository %s, job %s, action: %s",
            installation["id"],
            repository["full_name"],
            workflow_job["id"],
            payload.get("action", "unknown"),
        )

        await workflow_service.process_workflow_job_event(
            installation["id"], workflow_job, repository
        )

        try:
            await _broadcast_workflow_job(db, installation["id"], payload)
        except Exception as exc:
            logger.debug("SSE workflow_job broadcast failed: %s", exc)

        return {"status": "success", "message": "Processed workflow_job event"}

    if "installation" in payload and payload.get("action") in ["created", "deleted"]:
        logger.info(
            "Processing installation event: %s for installation %s",
            payload["action"],
            payload["installation"]["id"],
        )

        github_service = GitHubService(db=db)
        try:
            await github_service.handle_installation_event(payload=payload)
        except Exception as exc:
            if payload.get("action") != "created":
                raise
            db.rollback()
            logger.warning(
                "Falling back to installation upsert after installation handler failed: %s",
                exc,
            )
            ensure_installation_from_webhook_payload(db, payload)

        return {"status": "success", "message": "Processed installation event"}

    event_type = "unknown"
    if "workflow_run" in payload:
        event_type = "workflow_run"
    elif "workflow_job" in payload:
        event_type = "workflow_job"
    elif "installation" in payload:
        event_type = f"installation ({payload.get('action', 'unknown')})"

    logger.warning(
        "Unhandled event type: %s, action: %s, keys present: %s",
        event_type,
        payload.get("action", "N/A"),
        list(payload.keys()),
    )
    raise HTTPException(status_code=400, detail=f"Unhandled event type: {event_type}")
