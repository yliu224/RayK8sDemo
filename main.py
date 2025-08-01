import ray
import time
from counter import Counter

def counter_example(counter:Counter,count:int) -> None:
    for _ in range(count):
        counter.increment.remote()
        time.sleep(1)

if __name__ == "__main__":
    for i in range(10):
        print(f"Haha {i}")
        time.sleep(1)
    ray.init()
    print("Ray start")
    counter_a = Counter.remote("A")
    counter_b = Counter.remote("B")

    counter_example(counter_a,5)
    counter_example(counter_b,10)

    final_value = ray.get(counter_a.get.remote())
    print(f"counter_a Final counter value: {final_value}")

    final_value = ray.get(counter_b.get.remote())
    print(f"counter_a Final counter value: {final_value}")

