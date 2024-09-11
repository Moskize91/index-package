class Service:
  def __init__(
    self,
    sources: dict[str, str],
  ):
    self._sources: dict[str, str] = sources.copy()