# Velox cannot even correctly read the LAION data in Parquet...
# I tried to use let it read ORC then...

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.orc as orc

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
    orc.write_table(table, orc_path)
    
    print(f"Successfully converted {parquet_path} to {orc_path}")

# Example usage
if __name__ == "__main__":
    convert_parquet_to_orc("parquet/merged_8M.parquet", "orc/merged_8M.orc")