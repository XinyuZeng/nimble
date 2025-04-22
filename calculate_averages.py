#!/usr/bin/env python3

def calculate_average_from_mem_orc():
    total = 0
    count = 0
    
    try:
        with open('mem_orc.log', 'r') as file:
            for line in file:
                line = line.strip()
                if line and line.isdigit():
                    total += int(line)
                    count += 1
        
        if count > 0:
            average = total / count
            print(f"Average number in mem_orc.log: {average}")
        else:
            print("No valid numbers found in mem_orc.log")
    except FileNotFoundError:
        print("mem_orc.log file not found")

def calculate_stream_averages():
    # Use a dictionary to track all streams
    stream_data = {}  # {stream_num: {'total': X, 'count': Y}}
    
    try:
        with open('read_stream_size.log', 'r') as file:
            for line in file:
                line = line.strip()
                # Format appears to be "stream: N offset: X length: Y"
                if line.startswith('stream:'):
                    parts = line.split()
                    if len(parts) >= 6 and parts[0] == 'stream:' and parts[2] == 'offset:' and parts[4] == 'length:':
                        try:
                            stream_num = int(parts[1])
                            length = int(parts[5])
                            
                            # Initialize stream data if not exists
                            if stream_num not in stream_data:
                                stream_data[stream_num] = {'total': 0, 'count': 0}
                            
                            # Update totals
                            stream_data[stream_num]['total'] += length
                            stream_data[stream_num]['count'] += 1
                        except ValueError:
                            continue
        
        # Calculate and print averages for all streams
        print("\nAverage length for all streams:")
        for stream_num in sorted(stream_data.keys()):
            if stream_data[stream_num]['count'] > 0:
                avg = stream_data[stream_num]['total'] / stream_data[stream_num]['count']
                print(f"Stream {stream_num}: {avg}")
            else:
                print(f"Stream {stream_num}: No data found")
                
    except FileNotFoundError:
        print("read_stream_size.log file not found")

if __name__ == "__main__":
    print("Calculating averages from log files...")
    calculate_average_from_mem_orc()
    calculate_stream_averages() 