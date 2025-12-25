class Value:
    def __init__(self, value: str, timestamp: float=None):
        self.value: str = value
        self.dependencies: dict = {}
        self.timestamp: float = timestamp

    def get_value(self) -> str:
        return self.value
    
    def set_value(self, val: str) -> None:
        self.value = val

    def dep_length(self) -> int:
        return len(self.dependencies)

    def get_deps(self) -> dict:
        return self.dependencies
    
    def get_clock_at(self, key: str) -> str:
        return self.dependencies[key]

    def add_dep(self, key: str, val: str) -> None:
        self.dependencies[key] = val

    def set_deps(self, dep: dict) -> None:
        self.dependencies = dep

    def get_time(self) -> float:
        return self.timestamp
    
    def set_time(self, timestamp: float) -> None:
        self.timestamp = timestamp
    
    def to_dict(self) -> dict:
        return {
            "value": self.get_value(),
            "timestamp": self.timestamp,
            "dependencies": self.dependencies,
        }
    
    def __str__(self) -> str:
        return str(self.to_dict())
