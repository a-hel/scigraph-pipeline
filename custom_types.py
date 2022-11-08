from typing import Callable, Generator, Dict
from pony.orm.core import Entity, Database


DbTable = Entity
Record = Entity
Records = Generator[Record, None, None]

RawRecords = Generator[Dict, None, None]

PipelineFunc = Callable[[Records], dict]
