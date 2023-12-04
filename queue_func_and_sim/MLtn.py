import numpy as np
import pandas as pd
import simpy
import csv
import os
import random
import sys

mu = 1
customers = 5000
queue = []

class Store:
    def __init__(self, env, capacity):
        self.env = env
        self.simpy_res = simpy.Resource(env, capacity)

    def serving(self, joblength):
        yield self.env.timeout(joblength)

class Customer:
    def __init__(self, env, name, joblength):
        self.env = env
        self.name = name
        self.joblength = joblength

    def customer_serve(self, store):
        arrival = self.env.now
        queue.append(self.name)
        with store.simpy_res.request() as req:
            yield req
            queue.remove(self.name)
            yield self.env.process(store.serving(self.joblength))
            save_results(store.simpy_res.capacity, arrival, self.env.now - arrival)

def init_env(env, run, rho, capacity):
    store = Store(env, capacity)
    counter = 0
    while True:
        joblength, interval = mu_longtail(capacity, rho)
        customer = Customer(env, f'customer{counter}', joblength)
        env.process(customer.customer_serve(store))
        yield env.timeout(interval)
        counter += 1

def mu_longtail(capacity, rho):
    x = np.random.random()
    if x < 0.75:
        joblength = random.expovariate(1 / mu)
    else:
        joblength = random.expovariate(1 / (5 * mu))

    interval = random.expovariate(rho * capacity * 0.8)
    return joblength, interval


def save_results(capacity, arrival, waiting_time):
    file_path = os.path.join("data", f"mlt{capacity}_t.csv")
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([arrival, waiting_time, len(queue)])


# function to save in csv file in batches
def batch_process(rho, capacity):
    file_path = os.path.join("data", f"mlt{capacity}_t.csv")
    df = pd.read_csv(file_path)
    df.columns = ["arrival", "waits", "tot_length"]
    results_path = os.path.join("data", f"mlt{capacity}_means.csv")

    for batch in range(20):
        batch_df = df[(df["arrival"] > batch * 500 + 200) & (df["arrival"] < (batch + 1) * 500)]
        mean_waiting = batch_df["waits"].mean()
        mean_queue = batch_df["tot_length"].mean()

        with open(results_path, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([rho, batch, mean_waiting, mean_queue])

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
