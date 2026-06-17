import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session


from app.db.session import get_db
from app.db.models.installation import Installation
from app.db.models.job import Job
from app.db.models.runner import Runner
from app.schemas.user import User
from app.schemas.org import Runners
from app.api.dependencies import get_current_user
from app.services.runner_service import RunnerService, is_self_hosted_runner

router = APIRouter()

logger = logging.getLogger(__name__)


def _installation_ids_for_user(user: User, db: Session) -> list[int]:
    if user.get("organization_id"):
        installations = (
            db.query(Installation)
            .filter(Installation.organization_id == user["organization_id"])
            .all()
        )
        if installations:
            return [installation.installation_id for installation in installations]

    installations = db.query(Installation).filter(Installation.active).all()
    return [installation.installation_id for installation in installations]


def _active_installation_ids(db: Session) -> list[int]:
    installations = db.query(Installation).filter(Installation.active).all()
    return [installation.installation_id for installation in installations]


def _runners_for_installations(db: Session, installation_ids: list[int]) -> list[Runner]:
    if not installation_ids:
        return []

    return (
        db.query(Runner)
        .filter(Runner.installation_id.in_(installation_ids))
        .all()
    )


def _self_hosted_runners(runners: list[Runner]) -> list[Runner]:
    return [
        runner
        for runner in runners
        if is_self_hosted_runner(runner.name, runner.labels)
    ]


async def _backfill_runners_from_jobs(db: Session, installation_ids: list[int]):
    if not installation_ids:
        return

    jobs = (
        db.query(Job)
        .filter(Job.installation_id.in_(installation_ids))
        .filter(Job.raw_data.isnot(None))
        .order_by(Job.updated_at.desc())
        .limit(500)
        .all()
    )
    runner_service = RunnerService(db)

    for job in jobs:
        workflow_job = job.raw_data or {}
        if not workflow_job.get("runner_name"):
            continue

        runner_data = await runner_service.extract_runner_from_job_webhook(
            installation_id=job.installation_id,
            workflow_job=workflow_job,
            action=workflow_job.get("status") or "updated",
        )
        if not runner_data:
            continue

        runner = (
            db.query(Runner)
            .filter(
                Runner.installation_id == job.installation_id,
                Runner.runner_id == str(runner_data["id"]),
            )
            .first()
        )
        if runner and job.runner_id != runner.id:
            job.runner_id = runner.id
            db.add(job)

    db.commit()


# TODO: Rework this. Need to figure out how to collect better stats about the runner
# TODO: Create response object
@router.get("/runners")
async def get_organization_runners(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retrieve information about self-hosted runners for the authenticated user's organization.

    This endpoint provides details about the self-hosted runners associated with the user's
    organization, including the total number of runners, their statuses (online, offline, busy),
    and a list of all runners.

    Args:
        user (User): The authenticated user (injected via dependency).
        db (Session): The database session (injected via dependency).

    Returns:
        dict: A dictionary containing the following keys:
            - total_runners (int): The total number of self-hosted runners in the organization.
            - runners (list[Runners]): A list of self-hosted runners with detailed information.
            - online (int): The number of self-hosted runners currently online.
            - offline (int): The number of self-hosted runners currently offline.
            - busy (int): The number of self-hosted runners currently busy.

    Raises:
        HTTPException: If no installation is found for the user's organization (404).
    """
    installation_ids = _installation_ids_for_user(user, db)

    if not installation_ids:
        logger.warning(f"No installation found for {user}")
        raise HTTPException(status_code=404, detail="No installation found")

    await _backfill_runners_from_jobs(db, installation_ids)

    runners = _self_hosted_runners(_runners_for_installations(db, installation_ids))
    active_installation_ids = _active_installation_ids(db)
    if not runners and set(active_installation_ids) != set(installation_ids):
        await _backfill_runners_from_jobs(db, active_installation_ids)
        runners = _self_hosted_runners(
            _runners_for_installations(db, active_installation_ids)
        )

    total_runners = len(runners)

    online_count = sum(1 for runner in runners if runner.status == "online")

    offline_count = sum(1 for runner in runners if runner.status == "offline")

    busy_count = sum(1 for runner in runners if runner.busy)

    runners_list = [Runners.from_orm(r) for r in runners]

    return {
        "total_runners": total_runners,
        "runners": runners_list,
        "online": online_count,
        "offline": offline_count,
        "busy": busy_count,
    }
