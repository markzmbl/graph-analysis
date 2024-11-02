import time
import psutil
from pathlib import Path
from typing import Generator, Iterable, Any


def get_drive() -> Path:
    return Path("16TB")


def iteration_logging(
    generator: Iterable[Any],
    log_path: Path,
    log_interval: int = 1,
    max_time: int = 60 * 60
) -> Generator[Any, None, None]:
    """
    Tracks iterations over a generator and logs progress every log_interval seconds,
    along with memory usage of the program. Stops the iteration if the maximum time
    is reached.

    Args:
        generator (Iterable): The generator that yields the items to iterate over.
        log_path (Path): The path to save the log file.
        log_interval (int): The time interval in seconds to log progress.
        max_time (int): The maximum time in seconds to allow iteration.
    """
    # Initialize timing details
    start_time = time.time()  # Record the starting time
    last_log_time = start_time  # Keep track of the last log time
    total_iterations = 0  # Total iterations done so far
    process = psutil.Process()  # To track memory usage of the current process

    with log_path.open("w") as log_file:
        # Header for the log file
        log_file.write("time_seconds,iterations_total,iterations_rate,memory_usage_bytes\n")

        for item in generator:
            current_time = time.time()
            total_iterations += 1

            # Check if maximum time has been reached
            if current_time - start_time >= max_time:
                break  # Stop iteration if the maximum time is exceeded

            # Check if the log interval has passed
            if current_time - last_log_time >= log_interval:
                elapsed_time = current_time - start_time
                rate = total_iterations / elapsed_time  # Iterations per second

                # Get the memory usage in bytes
                memory_usage_bytes = process.memory_info().rss

                # Log progress: time, iterations, rate, memory usage
                line = f"{elapsed_time},{total_iterations},{rate},{memory_usage_bytes}\n"
                log_file.write(line)

                last_log_time = current_time  # Reset the last log time

            yield item  # Yield the item as usual for normal iteration flow