from enum import Enum


class DocTimeStamp(Enum):

    NO_TIMESTAMP = "no"        # Don't add a timestamp
    DOC_TIMESTAMP = "doc"      # add a timestamp for each doc created
    BATCH_TIMESTAMP = "batch"  # add a timestamp for each batch created

    def __str__(self):
        return self.value
