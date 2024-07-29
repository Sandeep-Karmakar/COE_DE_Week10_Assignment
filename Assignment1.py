import random
import time

def generate_random_number():
    return random.randint(50, 100)

def stream_random_numbers():
    while True:
        random_number = generate_random_number()
        print(f"Generated number: {random_number}")
        time.sleep(2)

if __name__ == "__main__":
    stream_random_numbers()
