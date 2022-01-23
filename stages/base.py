from typing import Iterable
from pony.orm import db_session


class PipelineStep:
    def _run(self, *args, **kwargs):
        raise NotImplementedError()

    def _run_once(self, *args, **kwargs):
        raise NotImplementedError()

    def run(self, data, *args, **kwargs):
        yield from self._run(data, *args, **kwargs)

    def run_once(self, data, *args, **kwargs):
        pass

    def apply(self, db=None, data=None, run_all=False):
        downstream = self.downstream
        if not isinstance(downstream, dict):
            downstream = {downstream: downstream}
        try:
            downstream = {k: getattr(db, v) for k, v in downstream.items()}
        except AttributeError:
            raise AttributeError(
                "The database is missing the downstream table '%s'." % self.downstream
            )
        if not isinstance(downstream, dict):
            downstream = {downstream: downstream}
        if data is None:
            try:
                upstream = getattr(db, self.upstream)
            except AttributeError:
                raise AttributeError(
                    "The database is missing the upstream table '%s'." % self.upstream
                )
            with db_session:
                data = db.get_records(
                    table=upstream, run_all=run_all, downstream=downstream
                )

        with db_session:
            for e, elem in enumerate(self.run(data)):
                if not e % 100:
                    print("Processing entry no %s." % e)

                for k, v in downstream.items():
                    items = elem.get(k, None)
                    if isinstance(items, dict):
                        items = [items]
                    if items is None:
                        continue
                    new_record = db.add_record(items, table=v)
                    yield new_record

    def as_func(self, data):
        def func():
            return self.run(data)

        return func
