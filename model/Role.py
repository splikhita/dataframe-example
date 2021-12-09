from dataclasses import dataclass
@dataclass(frozen=True)
class Role(object):
    id: int
    jobRole: str