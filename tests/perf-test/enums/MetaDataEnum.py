from enum import Enum

# db数据类型
class JobStatusEnum(Enum):
    CleaningUp = "1"
    Installing = "2"
    Running = "3"
    Blocked = "4"
    Failed = "5"
    Completed = "6"



