from typing import Callable, Generator, Optional, Dict, List
from contextlib import contextmanager

from logging import Logger
from pony.orm import db_session  # TODO: factor out somehow

logger = Logger()


class PipelineStep:
    def __init__(
        self,
        fn: Callable[[Generator], Generator],
        db: Optional["Database"],
        upstream: "db.Entity" = None,
        downstream: Optional["db.Entity"] = None,
        mode: str = "unprocessed",
    ):
        self.fn = fn
        self.db = db
        if (upstream or downstream) and not self.db:
            raise AttributeError(
                "You must specify a database if you want to use up- or downstream tables."
            )
        self.upstream = self._resolve_table_names(upstream)
        self.downstream = self._resolve_table_names(downstream)

    def _count_upstream_rows(self):
        return self.upstream.select().count()

    def _resolve_table_names(self, table_id):
        if isinstance(table_id, str):
            table_id = getattr(self.db, table_id)
        elif isinstance(table_id, list):
            table_id = [
                getattr(self.db, tbl_id) if isinstance(tbl_id, str) else tbl_id
                for tbl_id in table_id
            ]
        else:
            raise ValueError("Table names must be of type String or EntityMeta")
        return table_id

    @contextmanager
    def runner(self, write=True, exists=True, run_all=False):
        if write and not self.downstream:
            raise AttributeError(
                "You must specify a downstream table if you want to write your results."
            )

        def run_full(order_by=None):
            if self.upstream:
                all_data = self.db.get_records(
                    table=self.upstream,
                    run_all=True,
                    downstream=self.downstream,
                    prefetch=self.prefetch,
                    order_by=order_by,
                )
            yield from run(all_data)
            # for data in all_data:
            #    yield from run(data)

        def run_one(data):
            if self.upstream:
                data = self.db.get_by_id(table=self.upstream, id=data)
            yield from run(data)

        @db_session
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

    def run_all(
        self,
        data: Generator[Dict, None, None],
        write: bool = True,
        order_by: str = None,
    ):

        with self.runner(run_all=True, write=write) as run:
            result = run(order_by=order_by)
        return result

    def as_func(self, data, write):
        def func():
            return self.run_all(data, write)

        return func
