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
        try:
            downstream = getattr(db, self.downstream)
        except AttributeError:
            raise AttributeError("The database is missing the downstream table '%s'." % self.downstream)
        if data is None:
            try:
                upstream = getattr(db, self.upstream)
            except AttributeError:
                raise AttributeError("The database is missing the upstream table '%s'." % self.upstream)
            data = db.get_records(table=upstream, run_all=run_all, downstream=downstream)
        
        
        for e, elem in enumerate(self.run(data)):
            if not e % 100:
                print("Processing entry no %s." % e)
            new_record = db.add_record([elem], table=downstream)
            yield new_record

    def as_func(self, data):
        def func():
            return self.run(data)
        return func

