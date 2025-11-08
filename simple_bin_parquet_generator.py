import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def generate_skewed_parquet():

    # Number of rows - change this to any reasonable number, assume 50000 rows, or it can be sampled later
    num_of_rows = 50000

    # 95% of the rows will have IDs selected with probaboilites (defined with p) from a set of {16, 17, 18, 19, 20} (any random 5 IDs)
    #common_PULocations = np.random.choice(range(16, 21),
    #        size=int(num_of_rows * 0.95), p=[0.5, 0.2, 0.1, 0.1, 0.1])
    
    common_PULocations = np.full(int(num_of_rows * 0.60), 777)


    # Select randomly from a range of 1000 - 9999 for the remainder of the 5% pickup locations
    #rear_PULocations = np.random.choice(range(1000, 9999),
    #        size=int(num_of_rows * 0.05), replace=False)  # I chose no repeats i.e. replace=False

    # In BOTH skewed file generations
    rear_PULocations = np.random.choice(
    range(10_000, 50_000),      # SAME range in both files
    size=int(num_of_rows * 0.40),
    replace=True                # allow repeats so overlap occurs
)

    # Now, concatenate both of them and shuffle them - loop - 10 times
    puloc_ids = np.concatenate([common_PULocations, rear_PULocations])
    for i in range(10):
        np.random.shuffle(puloc_ids)

    # Making a fare-amount collection with values from 10 and 100, uniform dist - doesn't matter atp
    fare_amt = np.random.uniform(10.0, 100.0, size=num_of_rows)

    # Making the dataframe and then turning it into a parquet file
    df = pd.DataFrame({'PULocationID': puloc_ids,
                      'fare_amount': fare_amt})

    # This will write the file in the current directory (pwd)
    file_name = 'highly_skewed_50k_rows_R.parquet'
    pq.write_table(pa.Table.from_pandas(df), file_name)


def generate_unif_dist_parquet():

    # Number of rows - change this to any reasonable number, assume 50000 rows, or it can be sampled later
    num_of_rows = 50000

    # Expected to be Uniformaly spread out if probs not assigned
    puloc_ids = np.random.choice(range(10, 6000), size=num_of_rows)

    # Making a fare-amount collection with values from 10 and 100, uniform dist - doesn't matter atp
    fare_amt = np.random.uniform(10.0, 100.0, size=num_of_rows)

    # Making the dataframe and then turning it into a parquet file
    df = pd.DataFrame({'PULocationID': puloc_ids,
                      'fare_amount': fare_amt})

    # This will write the file in the current directory (pwd)
    file_name = 'uniformly_distributed_50k_rows.parquet'
    pq.write_table(pa.Table.from_pandas(df), file_name)

def generate_skew_for_sortmerge(filename: str,
                                num_rows: int = 6000,
                                hot_ratio: float = 0.70,
                                hot_key: int = 777,
                                rear_key: int = 888):
    # 70% on the same hot key in BOTH files
    hot = np.full(int(num_rows * hot_ratio), hot_key)
    # 30% on a single shared rear key in BOTH files (maximizes overlap without making hot_share ~1.0)
    rear = np.full(num_rows - len(hot), rear_key)

    puloc_ids = np.concatenate([hot, rear])
    np.random.shuffle(puloc_ids)  # order doesn't matter for your code

    fare_amt = np.random.uniform(10.0, 100.0, size=num_rows)

    df = pd.DataFrame({"PULocationID": puloc_ids, "fare_amount": fare_amt})
    pq.write_table(pa.Table.from_pandas(df), filename)
    print(f"✅ wrote {filename}")

def main():

    generate_skew_for_sortmerge("skew_L.parquet")
    generate_skew_for_sortmerge("skew_R.parquet")

    # Take the skew or non-skew option from the user --->
    while True:
        op = input("Enter 'skew' or 'unif': ")
        if op == 'skew':
            generate_skewed_parquet()
            break
        elif op == 'unif':
            generate_unif_dist_parquet()
            break
        else:
            continue


if __name__ == "__main__":
    main()

