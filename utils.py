import time
import psutil
from pathlib import Path

def iteration_logging(generator, log_path: Path, log_interval=1):
    """
    Tracks iterations over a generator and logs progress every log_interval seconds,
    along with memory usage of the program.

    Args:
        generator (iterable): The generator that yields the items to iterate over.
        log_interval (int): The time interval in seconds to log progress.
    """
    # Initialize timing details
    start_time = time.time()  # Record the starting time
    last_log_time = start_time  # Keep track of the last log time
    total_iterations = 0  # Total iterations done so far
    process = psutil.Process()  # To track memory usage of the current process

    with Path(log_path).open("w") as log_file:
        # Header for the log file
        print("time_seconds,iterations_total,iterations_rate,memory_usage_bytes", file=log_file)

        for item in generator:
            total_iterations += 1
            current_time = time.time()

            # Check if the log interval has passed
            if current_time - last_log_time >= log_interval:
                elapsed_time = current_time - start_time
                rate = total_iterations / elapsed_time  # Iterations per second

                # Get the memory usage in MB
                memory_usage_bytes = process.memory_info().rss

                # Log progress: time, iterations, rate, memory usage
                line = f"{elapsed_time},{total_iterations},{rate},{memory_usage_bytes}"
                print(line, file=log_file)

                last_log_time = current_time  # Reset the last log time

            yield item  # Yield the item as usual for normal iteration flow