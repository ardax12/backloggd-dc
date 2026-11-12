"""Discord announcement state layered over the SDK's scraped activity model."""
from dataclasses import asdict, dataclass
from typing import Optional
from backloggd import ActivityEntry


@dataclass(slots=True, frozen=True)
class FeedEvent(ActivityEntry):
    played_date: Optional[str] = None
    platform: Optional[str] = None
    session_labels: tuple[str, ...] = ()
    event_ids: tuple[str, ...] = ()
    previous_rating: Optional[float] = None
    source: str = "activity"

    @classmethod
    def from_activity(cls, entry: ActivityEntry) -> "FeedEvent":
        return entry if isinstance(entry, cls) else cls(**asdict(entry))
