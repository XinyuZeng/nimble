# Velox cannot even correctly read the LAION data in Parquet...
# I tried to use let it read ORC then...

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as orc
import os

def convert_parquet_to_orc(parquet_path, orc_path):
    """
    Convert a Parquet file to ORC format.
    
    Parameters:
    -----------
    parquet_path : str
        Path to the input Parquet file
    orc_path : str
        Path where the output ORC file will be saved
    """
    # Read the Parquet file into a PyArrow Table
    table = pq.read_table(parquet_path)
    
    # Write the PyArrow Table to ORC format
    orc.write_table(table, orc_path, compression="SNAPPY", dictionary_key_size_threshold=0.8, stripe_size=256 * 1024 * 1024)
    
    print(f"Successfully converted {parquet_path} to {orc_path}")

# Example usage
if __name__ == "__main__":
    convert_parquet_to_orc("/mnt/nvme0n1/xinyu/laion/parquet/merged_8M.parquet", "/mnt/nvme0n1/xinyu/laion/orc/merged_8M_256MBStripe.orc")
    
    # parquets = [
    #     "/mnt/nvme0n1/xinyu/data/parquet/bi.parquet",
    #     "/mnt/nvme0n1/xinyu/data/parquet/core.parquet",
    #     "/mnt/nvme0n1/xinyu/data/parquet/geo.parquet",
    #     "/mnt/nvme0n1/xinyu/data/parquet/log.parquet",
    #     "/mnt/nvme0n1/xinyu/data/parquet/ml.parquet",
    #     "/mnt/nvme0n1/xinyu/data/parquet/classic.parquet",
    #     "/mnt/nvme0n1/xinyu/tpch/parquet/lineitem_duckdb_double.parquet",
    #     "/mnt/nvme0n1/xinyu/clickbench/parquet/hits_8M.parquet"
    #   ]
    
    # for parquet in parquets:
    #     orc_path = parquet.replace("/parquet", "/orc_cpp").replace(".parquet", ".orc")
    #     os.makedirs(os.path.dirname(orc_path), exist_ok=True)
    #     convert_parquet_to_orc(parquet, orc_path)
