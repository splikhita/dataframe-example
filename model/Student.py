from dataclasses import dataclass
@dataclass(frozen=True)
class Student(object):
    firstname: str
    lastname: str
    school: str
    studentno: int