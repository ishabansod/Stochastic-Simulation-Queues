import csv
import simpy
import pandas as pd
import os
import random
import sys

mu = 1
customers = 5000
queue = []

class Booth:
    def __init__(self, env, capacity):
        self.env = env
        self.simpy_res = simpy.Resource(env, capacity)

    def serving(self):
        yield self.env.timeout(mu)

class Customer:
    def __init__(self, env, name):
        self.name = name
        self.env = env

    def customer_serve(self, booth):
        arrival = self.env.now
        queue.append(self.name)
        with booth.simpy_res.request() as req:
            yield req
            queue.remove(self.name)
            yield self.env.process(booth.serving())
            save_results(booth.simpy_res.capacity, arrival, self.env.now - arrival)

def init_env(env, run, rho, capacity):
    booth = Booth(env, capacity)
    counter = 0
    while True:
        customer = Customer(env, f'customer{counter}')
        env.process(customer.customer_serve(booth))
        yield env.timeout(random.expovariate(capacity * rho))
        counter += 1

def save_results(capacity, arrival, waiting_time):
    file_path = os.path.join("data", f"md{capacity}_t.csv")
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([arrival, waiting_time, len(queue)])

# function to save in csv file in batches
def batch_process(rho, capacity):
    file_path = os.path.join("data", f"md{capacity}_t.csv")
    df = pd.read_csv(file_path)
    df.columns = ["arrival", "waits", "tot_length"]
    results_path = os.path.join("data", f"md{capacity}_means.csv")

    for i in range(20):
        batch_df = df[(df["arrival"] > i * 500 + 200) & (df["arrival"] < (i + 1) * 500)]
        mean_waiting = batch_df["waits"].mean()
        mean_queue = batch_df["tot_length"].mean()

        with open(results_path, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([rho, mean_waiting, mean_queue])
    os.remove(file_path)

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
