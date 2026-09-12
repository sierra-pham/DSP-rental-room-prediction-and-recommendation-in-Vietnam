from abc import ABC, abstractmethod


class Parser(ABC):
    source: str

    @abstractmethod
    def parse(self, html: str, url: str) -> dict:
        """Return a dict matching the silver.listings schema. Never raises on
        missing optional fields — returns None for them instead."""
