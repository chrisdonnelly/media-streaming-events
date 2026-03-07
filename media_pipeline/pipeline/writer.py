from abc import ABC, abstractmethod

from media_pipeline.pipeline.models import FeatureRecord


class EventWriter(ABC):
    @abstractmethod
    def write(self, record: FeatureRecord) -> None: ...
