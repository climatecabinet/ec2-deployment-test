from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor, as_completed


def run_with_pool(worker, work_items, update_callback=None, db=None, chunksize=None):
    """ Maps a worker function onto a list of work items using a pool of concurrent workers"""

    if db:
        db.disconnect(quiet=True)

    chunk_size = len(work_items) if not chunksize else chunksize
    items_handled = 0
    batch_ind = 0

    while(batch := work_items[batch_ind: batch_ind+chunk_size]):
        batch_ind += chunk_size

        # design for ProcessPoolExecutor code snippet from https://stackoverflow.com/a/47108581
        with Manager() as manager:
            lock = manager.Lock()  # use of Manager.Lock() inspired by is.gd/IjgMVc

            with ProcessPoolExecutor(max_workers=6) as pool:

                if update_callback:
                    update_callback(items_handled)

                futures = [pool.submit(worker, item, lock) for item in batch]

                for f in as_completed(futures):
                    if f.exception() is not None:
                        for f in futures:
                            f.cancel()
                        break
                    else:
                        items_handled += 1
                        if update_callback:
                            update_callback(items_handled)

                [f.result() for f in futures]

    if db:
        db.connect(quiet=True)
