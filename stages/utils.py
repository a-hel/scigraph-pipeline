import more_itertools

def batched(data, batch_size=100):
    yield from more_itertools.ichunked(data, batch_size)
