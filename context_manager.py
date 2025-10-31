import re


class ConversationContext:
    """
    Tracks current topics (case numbers, entities, or people) per thread.
    Provides context continuity between turns.
    """

    CASE_PATTERN = re.compile(r"\b\d{2,}[A-Z]{1,3}-\d{4,6}\b", re.IGNORECASE)

    def __init__(self):
        self.active_cases = []
        self.active_entities = []
        self.last_topic_type = None  # "case" | "entity" | None

    def extract_case_numbers(self, text: str):
        return self.CASE_PATTERN.findall(text.upper())

    def extract_entities(self, text: str):
        # You can expand this pattern later or connect to your search index
        known_entities = [
            "elon musk",
            "asg",
            "mercadito",
            "administración de servicios generales",
        ]
        text_lower = text.lower()
        return [e.title() for e in known_entities if e in text_lower]

    def update(self, text: str):
        """Update the context memory given the user's message."""
        cases = self.extract_case_numbers(text)
        entities = self.extract_entities(text)

        if cases:
            self.active_cases = cases
            self.active_entities = []
            self.last_topic_type = "case"
        elif entities:
            self.active_entities = entities
            self.active_cases = []
            self.last_topic_type = "entity"
        # No new topics → keep old ones

    def summarize(self) -> str:
        """Return a human-readable summary for LLM prompt injection."""
        if self.last_topic_type == "case" and self.active_cases:
            return f"Context: Current active case(s): {', '.join(self.active_cases)}"
        elif self.last_topic_type == "entity" and self.active_entities:
            return (
                f"Context: Current entity or person: {', '.join(self.active_entities)}"
            )
        return ""
