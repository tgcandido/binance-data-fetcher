import sys
import pandas as pd
import numpy as np

def main(file_name):
    df = pd.read_csv(file_name)
    values = df.to_numpy()

    last_id = values[0][1]

    for row in values[1:]:
        trade_id = row[1]
        if last_id + 1 != trade_id:
            print('last_id', last_id)
            print('trade_id', trade_id)
            print('inconsistent data')
            exit()
        last_id = trade_id
    
    print('data is OK!')

if __name__ == "__main__":
    file_name = sys.argv[1]
    main(file_name)