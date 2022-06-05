from typing import Callable, Generator, Optional, Dict, List
from contextlib import contextmanager


class PipelineStep:
    def __init__(
        self,
        fn: Callable[[Generator], Generator],
        db: Optional["Database"],
        upstream: "db.Entity" = None,
        downstream: Optional["db.Entity"] = None,
        prefetch: List["db.Entity"] = [],
        write=False,
    ):
        self.fn = fn
        self.db = db
        if (upstream or downstream) and not self.db:
            raise AttributeError(
                "You must specify a database if you want to use up- or downstream tables."
            )
        if isinstance(upstream, str):
            upstream = getattr(db, upstream)
            # if not isinstance(upstream, db.db.Entity):
            #    raise AttributeError("Database has no table '%s'" % upstream)
        if isinstance(downstream, str):
            downstream = getattr(db, downstream)
            # if not isinstance(downstream, db.db.Entity):
            #    raise AttributeError("Database has no table '%s'" % downstream)
        self.upstream = upstream
        self.downstream = downstream
        self.prefetch = prefetch

    @contextmanager
    def runner(self, write=True, exists=True, run_all=False):
        if write and not self.downstream:
            raise AttributeError(
                "You must specify a downstream table if you want to write your results."
            )

        def run_full():
            if self.upstream:
                all_data = self.db.get_records(table=self.upstream, run_all=True, downstream=self.downstream, prefetch=self.prefetch)
            yield from run(all_data)
            #for data in all_data:
            #    yield from run(data)

        def run_one(data):
            if self.upstream:
                data = self.db.get_by_id(table=self.upstream, id=data)
            yield from run(data)

        def run(data):
            result = self.fn(data)
            if write:
                for elem in result:
                    id_ = self.db.add_record(data=result, table=self.downstream)
                    elem.update({"id": id_})
                    yield elem
            else:
                yield from result

        if run_all:
            yield run_full
        else:
            yield run_one

    def run_once(
        self, data: Generator[Dict, None, None], write: bool = True
    ) -> Generator:
        record = next(data)
        with self.runner(write=write) as run:
            result = run(record)

        for res in result:
            if not res:
                continue
            yield res

    def run_all(self, data: Generator[Dict, None, None], write: bool = True):
        with self.runner(run_all=True, write=write) as run:
            result = run()
        return result

    def as_func(self, data, write):
        def func():
            return self.run_all(data, write)

        return func
