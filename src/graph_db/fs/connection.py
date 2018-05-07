import io
import os
import errno


def open_file(path: str, record_size: int) -> io.BufferedIOBase:
    if not os.path.exists(os.path.dirname(path)):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    if not os.path.exists(path):
        # touch file
        with open(path, "w"):
            pass
    return open(path, "r+b", buffering=1024 * record_size)