from typing import Callable, Generator
from pony.orm.core import Entity, Database


DbTable = Entity
Records = Generator[Entity, None, None]

PipelineFunc = Callable[[Records], dict]
