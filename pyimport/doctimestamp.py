from enum import Enum
from datetime import datetime, timezone


class DocTimeStamp(Enum):

    NO_TIMESTAMP = "no"        # Don't add a timestamp
    DOC_TIMESTAMP = "doc"      # add a timestamp for each doc created
    BATCH_TIMESTAMP = "batch"  # add a timestamp for each batch created

    def __str__(self):
        return self.value

    def __call__(self, doc: dict) -> dict:
        """Make the enum callable to add timestamps to documents."""
        if self == DocTimeStamp.DOC_TIMESTAMP:
            # Add a new timestamp for each document
            doc["timestamp"] = datetime.now(timezone.utc)
        elif self == DocTimeStamp.BATCH_TIMESTAMP:
            # This should have been set as a datetime object already
            # but if it's still the enum, add timestamp now
            if isinstance(self, DocTimeStamp):
                doc["timestamp"] = datetime.now(timezone.utc)
        # NO_TIMESTAMP does nothing
        return doc
