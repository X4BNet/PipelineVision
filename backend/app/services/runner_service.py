import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import redis

from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.core.config import settings


from app.db.models.installation import Installation
from app.db.models.runner import Runner
from app.db.models.job import Job
from app.db.models.account import Organization
from app.services.github_service import GitHubService
from app.db.session import SessionLocal


logger = logging.getLogger(__name__)


def _label_name(label) -> Optional[str]:
    if isinstance(label, str):
        return label
    if isinstance(label, dict):
        return label.get("name")
    return None


def has_self_hosted_label(labels: Optional[List]) -> bool:
    return any(_label_name(label) == "self-hosted" for label in labels or [])


def is_github_hosted_runner_name(name: Optional[str]) -> bool:
    return bool(name and name.strip().lower().startswith("github actions"))


def normalize_runner_name(name: Optional[str]) -> Optional[str]:
    return name.strip().lower() if name else None


def is_self_hosted_runner(name: Optional[str], labels: Optional[List]) -> bool:
    if has_self_hosted_label(labels):
        return True
    if is_github_hosted_runner_name(name):
        return False
    return bool(name)


class RunnerService:
    """
    Efficient Runner Service that reduces GitHub API calls and improves scalability.

    Features:
    - Extracts runner info from workflow_job webhooks
    - Smart differential sync with reduced API calls
    - Tracks runner activity to optimize polling
    - Uses Redis for caching (when available)
    """

    def __init__(self, db: Session, redis_client=None):
        self.db = db
        self.redis_client = redis_client
        self.github_service = GitHubService(db)

        self.active_runner_ttl = 300  # 5 minutes for active runners
        self.inactive_runner_ttl = 1800  # 30 minutes for inactive runners
        self.max_api_calls_per_hour = 4000  # Conservative limit

        self.api_call_count = 0
        self.api_reset_time = datetime.utcnow() + timedelta(hours=1)

    def _get_cache_key(self, installation_id: int) -> str:
        """Generate cache key for runner data."""
        return f"runners:installation:{installation_id}"

    def _get_runner_activity_key(self, installation_id: int) -> str:
        """Generate cache key for runner activity tracking."""
        return f"runner_activity:installation:{installation_id}"

    def _find_runner_by_name(
        self, installation_id: int, runner_name: Optional[str]
    ) -> Optional[Runner]:
        normalized_name = normalize_runner_name(runner_name)
        if not normalized_name:
            return None

        runners = (
            self.db.query(Runner)
            .filter(Runner.installation_id == installation_id)
            .all()
        )
        matching_runners = [
            runner
            for runner in runners
            if normalize_runner_name(runner.name) == normalized_name
        ]
        if not matching_runners:
            return None

        matching_runners = sorted(matching_runners, key=lambda runner: runner.id)
        canonical_runner = matching_runners[0]
        for duplicate_runner in matching_runners[1:]:
            canonical_runner = self._merge_runner_records(
                canonical_runner, duplicate_runner
            )
        return canonical_runner

    def _find_runner_by_identity(
        self, installation_id: int, runner_id: str, runner_name: Optional[str]
    ) -> Optional[Runner]:
        runner_by_name = self._find_runner_by_name(installation_id, runner_name)
        runner_by_id = (
            self.db.query(Runner)
            .filter(
                and_(
                    Runner.installation_id == installation_id,
                    Runner.runner_id == runner_id,
                )
            )
            .first()
        )

        if runner_by_name and runner_by_id and runner_by_name.id != runner_by_id.id:
            return self._merge_runner_records(runner_by_name, runner_by_id)

        return runner_by_name or runner_by_id

    def _merge_runner_records(self, canonical: Runner, duplicate: Runner) -> Runner:
        if canonical.id == duplicate.id:
            return canonical

        self.db.query(Job).filter(Job.runner_id == duplicate.id).update(
            {Job.runner_id: canonical.id},
            synchronize_session=False,
        )

        if not canonical.os and duplicate.os:
            canonical.os = duplicate.os
        if not canonical.architecture and duplicate.architecture:
            canonical.architecture = duplicate.architecture
        if not canonical.labels and duplicate.labels:
            canonical.labels = duplicate.labels
        if canonical.ephemeral is None and duplicate.ephemeral is not None:
            canonical.ephemeral = duplicate.ephemeral
        if duplicate.last_seen and (
            not canonical.last_seen or duplicate.last_seen > canonical.last_seen
        ):
            canonical.last_seen = duplicate.last_seen
        if duplicate.last_check and (
            not canonical.last_check or duplicate.last_check > canonical.last_check
        ):
            canonical.last_check = duplicate.last_check

        self.db.delete(duplicate)
        self.db.flush()
        return canonical

    def _apply_runner_data(self, runner: Runner, runner_data: Dict) -> bool:
        changes_made = False

        updates = {
            "runner_id": str(runner_data["id"]),
            "name": runner_data.get("name"),
            "status": runner_data.get("status"),
            "labels": runner_data.get("labels", []),
            "busy": bool(runner_data.get("busy", False)),
        }
        for optional_field in ("os", "architecture", "ephemeral"):
            if optional_field in runner_data:
                updates[optional_field] = runner_data[optional_field]

        for field, value in updates.items():
            if value is not None and getattr(runner, field) != value:
                setattr(runner, field, value)
                changes_made = True

        if changes_made or runner_data.get("extracted_from") == "webhook":
            runner.last_check = datetime.utcnow()
            if runner.status in ["online", "busy"]:
                runner.last_seen = datetime.utcnow()

        return changes_made

    async def extract_runner_from_job_webhook(
        self, installation_id: int, workflow_job: Dict, action: str
    ) -> Optional[Dict]:
        """
        Extract runner information from workflow_job webhook events.
        This is the most efficient way to get real-time runner updates.
        """
        try:
            runner_name = workflow_job.get("runner_name")
            runner_id = workflow_job.get("runner_id")
            labels = workflow_job.get("labels", [])

            if not runner_name:
                logger.debug(
                    f"No runner info in job webhook: {workflow_job.get('name')}"
                )
                return None

            if not is_self_hosted_runner(runner_name, labels):
                logger.debug(
                    f"Skipping GitHub-hosted runner {runner_name} from job webhook"
                )
                return None

            if not runner_id:
                runner_id = f"name:{runner_name}"

            runner_status = "online"
            runner_busy = False

            if action in ["queued", "in_progress"]:
                runner_busy = True
                runner_status = "online"
            elif action in ["completed", "cancelled"]:
                runner_busy = False
                runner_status = "online"

            runner_data = {
                "id": runner_id,
                "name": runner_name,
                "status": runner_status,
                "busy": runner_busy,
                "labels": labels,
                "last_seen": datetime.utcnow().isoformat(),
                "extracted_from": "webhook",
            }

            await self._update_runner_from_webhook(installation_id, runner_data)

            await self._mark_installation_active(installation_id)

            logger.info(
                f"Extracted runner {runner_name} from job webhook for installation {installation_id}"
            )
            return runner_data

        except Exception as e:
            logger.error(f"Error extracting runner from job webhook: {e}")
            return None

    async def _update_runner_from_webhook(
        self, installation_id: int, runner_data: Dict
    ):
        """Update runner data from webhook information."""
        try:
            runner_id = str(runner_data["id"])
            existing_runner = self._find_runner_by_identity(
                installation_id, runner_id, runner_data.get("name")
            )

            if existing_runner:
                self._apply_runner_data(existing_runner, runner_data)
                self.db.add(existing_runner)
            else:
                new_runner = Runner(
                    installation_id=installation_id,
                    runner_id=runner_id,
                    name=runner_data.get("name"),
                    status=runner_data.get("status", "online"),
                    busy=bool(runner_data.get("busy", False)),
                    labels=runner_data.get("labels", []),
                    os=None,
                    architecture=None,
                    ephemeral=False,
                    last_seen=datetime.utcnow(),
                    last_check=datetime.utcnow(),
                )
                self.db.add(new_runner)

            self.db.commit()

            if self.redis_client:
                cache_key = self._get_cache_key(installation_id)
                self.redis_client.delete(cache_key)

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating runner from webhook: {e}")

    async def _mark_installation_active(self, installation_id: int):
        """Mark installation as having recent runner activity."""
        if not self.redis_client:
            return

        try:
            activity_key = self._get_runner_activity_key(installation_id)
            self.redis_client.setex(
                activity_key, 3600, datetime.utcnow().isoformat()
            )  # 1 hour
        except Exception as e:
            logger.debug(f"Failed to mark installation active: {e}")

    async def _is_installation_active(self, installation_id: int) -> bool:
        """Check if installation has recent runner activity."""
        if not self.redis_client:
            return True  # Default to active if no Redis

        try:
            activity_key = self._get_runner_activity_key(installation_id)
            return self.redis_client.exists(activity_key)
        except Exception as e:
            logger.debug(f"Failed to check installation activity: {e}")
            return True

    async def smart_sync_runners(self) -> Dict[str, int]:
        """
        Intelligent runner synchronization that minimizes API calls.

        Returns:
            Dict with sync statistics
        """
        stats = {
            "installations_checked": 0,
            "installations_synced": 0,
            "runners_updated": 0,
            "api_calls_made": 0,
            "skipped_inactive": 0,
            "skipped_rate_limit": 0,
        }

        try:
            if self._should_throttle_api_calls():
                logger.warning("Throttling API calls due to rate limit")
                stats["skipped_rate_limit"] = -1
                return stats

            installations = self.db.query(Installation).all()
            stats["installations_checked"] = len(installations)

            for installation in installations:
                try:
                    if not await self._is_installation_active(
                        installation.installation_id
                    ):
                        if not await self._should_sync_inactive_installation(
                            installation.installation_id
                        ):
                            stats["skipped_inactive"] += 1
                            continue

                    organization = (
                        self.db.query(Organization)
                        .filter(Organization.id == installation.organization_id)
                        .first()
                    )

                    if not organization:
                        logger.warning(
                            f"No organization found for installation {installation.installation_id}"
                        )
                        continue

                    sync_result = await self._smart_sync_installation_runners(
                        installation.installation_id, organization.login
                    )

                    stats["installations_synced"] += 1
                    stats["runners_updated"] += sync_result.get("runners_updated", 0)
                    stats["api_calls_made"] += sync_result.get("api_calls", 0)

                except Exception as e:
                    logger.error(
                        f"Error syncing installation {installation.installation_id}: {e}"
                    )
                    continue

            logger.info(f"Smart sync completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Error in smart_sync_runners: {e}")
            return stats

    def _should_throttle_api_calls(self) -> bool:
        """Check if we should throttle API calls due to rate limiting."""
        now = datetime.utcnow()

        if now > self.api_reset_time:
            self.api_call_count = 0
            self.api_reset_time = now + timedelta(hours=1)

        return self.api_call_count >= self.max_api_calls_per_hour * 0.8  # 80% threshold

    async def _should_sync_inactive_installation(self, installation_id: int) -> bool:
        """Check if an inactive installation should be synced."""
        if not self.redis_client:
            return True

        try:
            last_sync_key = f"last_inactive_sync:installation:{installation_id}"
            last_sync = self.redis_client.get(last_sync_key)

            if not last_sync:
                self.redis_client.setex(
                    last_sync_key, 1800, datetime.utcnow().isoformat()
                )  # 30 min
                return True

            last_sync_time = datetime.fromisoformat(last_sync.decode())
            return datetime.utcnow() - last_sync_time > timedelta(minutes=30)

        except Exception as e:
            logger.debug(f"Error checking inactive sync: {e}")
            return True

    async def _smart_sync_installation_runners(
        self, installation_id: int, organization_name: str
    ) -> Dict[str, int]:
        """
        Smart sync for a single installation that minimizes API calls.
        """
        result = {"runners_updated": 0, "api_calls": 0}

        try:
            cached_runners = await self._get_cached_runners(installation_id)
            if cached_runners and await self._is_cache_valid(installation_id):
                logger.debug(
                    f"Using cached runner data for installation {installation_id}"
                )
                return result

            runners_data = await self.github_service.get_organization_runners(
                organization_name=organization_name, installation_id=installation_id
            )
            result["api_calls"] = 1
            self.api_call_count += 1

            updated_count = await self._update_runners(installation_id, runners_data)
            result["runners_updated"] = updated_count

            await self._cache_runners(installation_id, runners_data)

            logger.info(
                f"Smart sync completed for installation {installation_id}: {result}"
            )

        except Exception as e:
            logger.error(f"Error in smart sync for installation {installation_id}: {e}")

        return result

    async def _get_cached_runners(self, installation_id: int) -> Optional[List[Dict]]:
        """Get cached runner data."""
        if not self.redis_client:
            return None

        try:
            cache_key = self._get_cache_key(installation_id)
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            logger.debug(f"Cache read error: {e}")

        return None

    async def _is_cache_valid(self, installation_id: int) -> bool:
        """Check if cached data is still valid."""
        if not self.redis_client:
            return False

        try:
            cache_key = self._get_cache_key(installation_id)
            ttl = self.redis_client.ttl(cache_key)
            return ttl > 0
        except Exception as e:
            logger.debug(f"Cache validation error: {e}")
            return False

    async def _cache_runners(self, installation_id: int, runners_data: List[Dict]):
        """Cache runner data with appropriate TTL."""
        if not self.redis_client:
            return

        try:
            cache_key = self._get_cache_key(installation_id)

            ttl = (
                self.active_runner_ttl
                if await self._is_installation_active(installation_id)
                else self.inactive_runner_ttl
            )

            self.redis_client.setex(
                cache_key, ttl, json.dumps(runners_data, default=str)
            )
        except Exception as e:
            logger.debug(f"Cache write error: {e}")

    async def _update_runners(
        self, installation_id: int, runners_data: List[Dict]
    ) -> int:
        """Efficiently update runners with minimal database operations."""
        if not runners_data:
            return 0

        try:
            processed_runner_ids = set()
            processed_runner_names = set()
            updated_count = 0

            for runner in runners_data:
                runner_name = runner.get("name")
                runner_labels = runner.get("labels", [])
                if not is_self_hosted_runner(runner_name, runner_labels):
                    continue

                runner_id = str(runner["id"])
                processed_runner_ids.add(runner_id)
                normalized_name = normalize_runner_name(runner_name)
                if normalized_name:
                    processed_runner_names.add(normalized_name)

                runner_data = {
                    "id": runner_id,
                    "name": runner_name,
                    "os": runner.get("os"),
                    "status": runner.get("status"),
                    "busy": bool(runner.get("busy", False)),
                    "labels": runner_labels,
                    "ephemeral": bool(runner.get("ephemeral", False)),
                    "architecture": runner.get("architecture"),
                }

                existing_runner = self._find_runner_by_identity(
                    installation_id, runner_id, runner_name
                )
                if existing_runner:
                    if self._apply_runner_data(existing_runner, runner_data):
                        updated_count += 1
                    self.db.add(existing_runner)
                else:
                    new_runner = Runner(
                        installation_id=installation_id,
                        runner_id=runner_id,
                        name=runner_name,
                        os=runner_data["os"],
                        status=runner_data["status"],
                        busy=runner_data["busy"],
                        labels=runner_data["labels"],
                        ephemeral=runner_data["ephemeral"],
                        architecture=runner_data["architecture"],
                        last_seen=datetime.utcnow(),
                        last_check=datetime.utcnow(),
                    )
                    self.db.add(new_runner)
                    updated_count += 1
                self.db.flush()

            existing_runners = (
                self.db.query(Runner)
                .filter(Runner.installation_id == installation_id)
                .all()
            )
            for runner in existing_runners:
                runner_name = normalize_runner_name(runner.name)
                if (
                    runner.runner_id not in processed_runner_ids
                    and runner_name not in processed_runner_names
                    and runner.status != "offline"
                ):
                    runner.status = "offline"
                    runner.busy = False
                    runner.last_check = datetime.utcnow()
                    self.db.add(runner)
                    updated_count += 1

            self.db.commit()
            return updated_count

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating runners efficiently: {e}")
            return 0


class SmartRunnerScheduler:
    """
    Replacement for the current RunnerSyncScheduler with intelligent scheduling.
    """

    def __init__(self, redis_client=None):
        self.running = False
        self.redis_client = redis_client

        # Dynamic intervals based on activity
        self.min_interval = 30  # 30 seconds for very active installations
        self.max_interval = 300  # 5 minutes for inactive installations
        self.current_interval = 60  # Start with 1 minute

    async def start(self):
        """Start the smart scheduler."""
        if self.running:
            return

        self.running = True
        asyncio.create_task(self._run_smart_sync_loop())
        logger.info("Smart runner scheduler started")

    async def stop(self):
        """Stop the scheduler."""
        self.running = False
        logger.info("Smart runner scheduler stopped")

    async def _run_smart_sync_loop(self):
        """Intelligent sync loop that adapts to activity."""
        consecutive_no_updates = 0

        while self.running:
            try:
                db = SessionLocal()
                try:
                    runner_service = RunnerService(db, self.redis_client)
                    stats = await runner_service.smart_sync_runners()

                    # Adjust interval based on activity
                    if stats["runners_updated"] == 0:
                        consecutive_no_updates += 1
                        # Slow down if no updates for multiple cycles
                        if consecutive_no_updates >= 3:
                            self.current_interval = min(
                                self.current_interval * 1.2, self.max_interval
                            )
                    else:
                        consecutive_no_updates = 0
                        # Speed up if there are updates
                        self.current_interval = max(
                            self.current_interval * 0.8, self.min_interval
                        )

                    logger.debug(
                        f"Next sync in {self.current_interval} seconds. Stats: {stats}"
                    )

                finally:
                    db.close()

            except Exception as e:
                logger.error(f"Error in smart sync loop: {e}")
                # Back off on errors
                self.current_interval = min(
                    self.current_interval * 1.5, self.max_interval
                )

            await asyncio.sleep(self.current_interval)


# Create singleton instance (optional Redis integration)
try:

    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=int(settings.REDIS_PORT),
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    # Test connection
    redis_client.ping()
    logger.info(
        f"Redis connection established at {settings.REDIS_HOST}:{settings.REDIS_PORT} for runner caching"
    )
except Exception as e:
    redis_client = None
    logger.info(f"Running without Redis caching: {e}")

smart_runner_scheduler = SmartRunnerScheduler(redis_client)
