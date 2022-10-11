from typing import Callable, Generator, Optional, Dict, List
from contextlib import contextmanager
from enum import Enum

from tqdm import tqdm
from pony.orm import db_session  # TODO: factor out somehow

from utils.run_modes import RunModes
from utils.logging import PipelineLogger

logger = PipelineLogger("Pipeline")


class PipelineStep:
    def __init__(
        self,
        fn: Callable[[Generator], Generator],
        db: Optional["Database"],
        upstream: "db.Entity" = None,
        downstream: Optional["db.Entity"] = None,
        name: str = None,
        func_args: dict = {},
    ):
        self.fn = fn
        self.func_args = func_args
        self.db = db
        if (upstream or downstream) and not self.db:
            raise AttributeError(
                "You must specify a database if you want to use up- or downstream tables."
            )
        self.upstream = self._resolve_table_names(upstream)
        self.downstream = self._resolve_table_names(downstream)
        self.name = f"step_{name or self.fn.__name__}"

    def _count_upstream_rows(self, mode):
        if mode == RunModes.ONCE:
            return 1
        if self.downstream is None:
            return -1
        n_elems = self.db.count_records(
            table=self.upstream, downstream=self.downstream, mode=mode
        )
        return n_elems

    def _resolve_table_names(self, table_id):
        if table_id is None:
            return None
        elif isinstance(table_id, str):
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
    def runner(self, mode, order_by=None, write=True):
        if write and not self.downstream:
            raise AttributeError(
                "You must specify a downstream table if you want to write your results."
            )

        def run_full():
            if self.upstream:
                all_data = self.db.get_records(
                    table=self.upstream,
                    mode=mode,
                    downstream=self.downstream,
                    order_by=order_by,
                )
            yield from run(all_data)

        def run_one(id):
            if self.upstream:
                data = self.db.get_by_id(table=self.upstream, id=id)
            yield from run(data)

        @db_session
        def run(data):
            logger.info(f"Running step {self.name} (write = {write})")
            # logger.debug(f"{self.upstream._table_[-1]} -> {self.downstream._table_[-1]}")
            result = self.fn(data, **self.func_args)
            if write:
                for elem in result:
                    id_ = self.db.add_record(data=result, table=self.downstream)
                    elem.update({"id": id_})
                    yield elem
            else:
                yield from result

        if mode == RunModes.ONCE:
            yield run_one
        else:
            yield run_full

    def run_all(
        self,
        write: bool = True,
        mode: RunModes = RunModes.ALL,
        order_by: str = None,
    ):
        if not isinstance(mode, RunModes):
            try:
                mode = RunModes[mode.upper()]
            except KeyError:
                msg = f"Unknown mode '{mode}'. Allowed values are {', '.join(RunModes.__members__.keys())}"
                raise KeyError(msg)
        try:
            n_elems = self._count_upstream_rows(mode=mode)
        except AttributeError as e:
            logger.warning("Unable to count processable rows: %s" % e)
            n_elems = None
        with self.runner(write=write, mode=mode, order_by=order_by) as run:
            result = run()
        for elem in tqdm(result, total=n_elems, desc=self.name):
            yield elem

    def run_once(self, id: int, write: bool = True):

        with self.runner(write=write, mode=RunModes.ONCE, order_by=False) as run:
            result = run(id=id)
        return result

    def as_func(self, write, mode=RunModes.ALL, order_by=False):
        def func():
            return self.run_all(write, mode, order_by)

        return func
