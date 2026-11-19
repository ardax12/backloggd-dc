"""Discord announcement state layered over the SDK's scraped activity model."""
from dataclasses import asdict, dataclass
from typing import Optional

from backloggd import ActivityEntry

GROUPED_RATING_ID_PREFIX = "group_rating_"


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
        if isinstance(entry, cls):
            return entry
        fields = asdict(entry)
        fields.pop("grouped_games", None)
        return cls(**fields)
