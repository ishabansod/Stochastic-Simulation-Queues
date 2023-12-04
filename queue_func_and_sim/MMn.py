import pandas as pd
import simpy
import csv
import os
import random
import sys

mu = 1
customers = 5000
queue = []

class Booth:
    def __init__(self, env, capacity):
        self.env = env
        self.capacity = capacity
        self.simpy_res = simpy.Resource(env, capacity)

    def serving(self, joblength):
        yield self.env.timeout(joblength)


class Customer:
    def __init__(self, env, name, joblength):
        self.env = env
        self.name = name
        self.joblength = joblength

    def customer_serve(self, env, run, rho, booth):
        arrival = env.now
        queue.append(self.name)

        with booth.simpy_res.request() as req:
            yield req
            join_q = env.now
            queue.remove(self.name)
            yield env.process(booth.serving(self.joblength))
            save_results(booth.capacity, arrival, join_q - arrival)


def init_env(env, run, rho, capacity):
    booth = Booth(env, capacity)
    counter = 0

    while True:
        joblength = random.expovariate(mu)
        interval = random.expovariate(capacity * rho)
        customer = Customer(env, f'customer{counter}', joblength)
        env.process(customer.customer_serve(env, run, rho, booth))
        yield env.timeout(interval)
        counter += 1


def save_results(capacity, arrival, waiting_time):
    file_path = f"data/mm{capacity}_t.csv"
    with open(file_path, 'a') as results_file:
        writer = csv.writer(results_file)
        writer.writerow([arrival, waiting_time, len(queue)])

# function to save in csv file in batches
def batch_process(rho, capacity):
    df = pd.read_csv(f"data/mm{capacity}_t.csv")
    df.columns = ["arrival", "waits", "tot_length"]

    for i in range(20):
        mean_waiting_time = df[(df["arrival"] > i * 500 + 200) & (df["arrival"] < (i + 1) * 500)]["waits"].mean()
        mean_queue_length = df[(df["arrival"] > i * 500 + 200) & (df["arrival"] < (i + 1) * 500)]["tot_length"].mean()

        with open(f"data/mm{capacity}_means.csv", 'a') as results_file:
            writer = csv.writer(results_file)
            writer.writerow([rho, mean_waiting_time, mean_queue_length])

    os.remove(f"data/mm{capacity}_t.csv")


def main():
    if len(sys.argv) != 4:
        sys.exit(1)

    capacity = sys.argv[1]
    rho = sys.argv[2]
    run = sys.argv[3]

    env = simpy.Environment()
    env.process(init_env(env, run, rho, capacity))
    env.run(until=customers)
    batch_process(rho, capacity)

if __name__ == "__main__":
    main()