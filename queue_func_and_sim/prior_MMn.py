import sys
import pandas as pd
import os
import simpy
import random
import csv

mu = 1
customers = 5000
queue = []

#booth
class Booth():
    def __init__(self, env, capacity):
        self.env = env
        self.capacity = capacity
        self.simpy_res = simpy.PriorityResource(env, capacity=capacity)

    def serving(self, joblength):
        yield self.env.timeout(joblength)

#customers
class Customer():
    def __init__(self, env, name, joblength):
        self.env = env
        self.name = name
        self.joblength = joblength

    def customer_serve(self, env, run, rho, booth):
        arrival = env.now
        queue.append(self.name)

        with booth.simpy_res.request(priority=self.joblength) as req:
            yield req

            join_q = env.now
            queue.remove(self.name)

            yield env.process(booth.serving(self.joblength))

            with open("data/" + str(booth.capacity) + "_t.csv", 'a') as resultsFile:
                writer = csv.writer(resultsFile)

                writer.writerow([arrival, join_q - arrival, len(queue)])


def init_env(env, run, rho, capacity):
    booth = Booth(env, capacity)
    counter = 0

    while True:
        joblength = random.expovariate(mu)
        customer = Customer(env, f'customer{counter}', joblength)

        interval = random.expovariate(capacity*rho)

        env.process(customer.customer_serve(env, run, rho, booth))
        yield env.timeout(interval)
        counter+=1

# function to save in csv file in batches
def batch_process(rho, capacity):
    df = pd.read_csv("data/" + str(capacity) + "_t.csv")
    df.columns = ["arrival", "waits", "tot_length"]

    for i in range(20):
        df[(df["arrival"] > i * 500 + 200) & (df["arrival"] < (i + 1) * 500)]["waits"].mean()

        with open("data/" + str(capacity)  + "_means.csv", 'a') as resultsFile:
            writer = csv.writer(resultsFile)

            writer.writerow([rho,
                                df[(df["arrival"] > i * 500 + 200) & (df["arrival"] < (i + 1) * 500)]["waits"].mean(),
                                df[(df["arrival"] > i * 500 + 200) & (df["arrival"] < (i + 1) * 500)]["tot_length"].mean()])

    os.remove("data/" + str(capacity) + "_t.csv")

def main():
    if not len(sys.argv) == 4:
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