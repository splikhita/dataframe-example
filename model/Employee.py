from dataclasses import dataclass

@dataclass(frozen=True)
class Employee(object):
    id: int
    firstName: str
    lastName: str