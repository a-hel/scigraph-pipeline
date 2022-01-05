from typing import Callable, Generator, Optional, Dict
from contextlib import contextmanager

class PipelineStep:
    def __init__(self, fn: Callable[[Generator], Generator],
     db: Optional['Database'],
      upstream: 'db.Entity'=None, 
      downstream: Optional['db.Entity'] = None):
        self.fn = fn
        self.db = db
        if (upstream or downstream) and not self.db:
            raise AttributeError('You must specify a database if you want to use up- or downstream tables.')
        self.upstream = upstream
        self.downstream = downstream

    @contextmanager
    def runner(self, write=True, exists=True):
        if write and not self.downstream:
            raise AttributeError('You must specify a downstream table if you want to write your results.')
        def run(data):
            result = self.fn(data)
            if write:
                for elem in result:
                    id_ = self.db.add_record(data=result, table=self.downstream)
                    elem.update({"id": id_})
                    yield elem
            else:
                yield from result
        yield run


    def run_once(self, data: Generator[Dict, None, None], write: bool=True):
        with self.runner(write=write) as run:
            result = run(data)
                
        for res in result:
            if not res:
                continue
            break
        return res

    def run_all(self, data: Generator[Dict, None, None], write: bool=True):
        with self.runner(write=write) as run:
            result = run(data)
        return result

    def as_func(self, data, write):
        def func():
            return self.run_all(data, write)
        return func