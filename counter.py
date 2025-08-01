import ray

@ray.remote
class Counter:
    def __init__(self,name:str):
        self.name = name
        self.value = 0

    def increment(self):
        print(f"{self.name} current value is {self.value}")
        self.value += 1
        return self.value

    def get(self):
        return self.value
