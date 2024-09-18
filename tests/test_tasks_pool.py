import time
import unittest
import threading

from index_package.utils import assert_continue, TasksPool, InterruptException

class _SafeInt:
  def __init__(self, val: int):
    self.val = val
    self._lock = threading.Lock()

  @property
  def value(self) -> int:
    with self._lock:
      return self.val

  def inc(self, value: int):
    with self._lock:
      self.val += value

class TestTasksPool(unittest.TestCase):

  def test_running_util_success(self):
    count = _SafeInt(0)
    pool = TasksPool[int](
      max_workers=4,
      on_handle=lambda x: count.inc(x),
    )
    pool = pool.start()
    main_count = 0

    for i in range(100):
      success = pool.push(i)
      main_count += i
      self.assertTrue(success)
      if pool.is_interrupted:
        break

    self.assertEqual(count.value, main_count)

  def test_task_failure(self):
    pool = TasksPool[int](
      max_workers=2,
      print_error=False,
      on_handle=self._crash_handler,
    )
    pool = pool.start()
    inserted_integers: list[int] = []

    for i in range(100):
      success = pool.push(i)
      if not success:
        break
      inserted_integers.append(i)

    pool.complete()
    self.assertEqual(inserted_integers, [0, 1])

  def _crash_handler(self, _: int):
    time.sleep(0.3) # make sure not interrupting another task
    raise Exception("Task failed")

  def test_interrupt_task(self):
    pool = TasksPool[int](
      max_workers=2,
      print_error=False,
      on_handle=self._will_be_never_wake_up,
    )
    pool = pool.start()
    inserted_integers: list[int] = []
    thread = threading.Thread(target=lambda: self._to_interrupt(pool))
    thread.start()

    for i in range(100):
      success = pool.push(i)
      if not success:
        break
      inserted_integers.append(i)

    pool.complete()
    thread.join()

    self.assertEqual(inserted_integers, [0, 1, 2, 3, 4])

  def _will_be_never_wake_up(self, integer: int):
    if integer >= 3:
      time.sleep(0.3)

  def test_tasks_can_only_be_interrupted(self):
    pool = TasksPool[int](
      max_workers=2,
      print_error=False,
      on_handle=self._can_only_be_interrupted,
    )
    pool = pool.start()
    inserted_integers: list[int] = []
    thread = threading.Thread(target=lambda: self._to_interrupt(pool))
    thread.start()

    for i in range(100):
      success = pool.push(i)
      if not success:
        break
      inserted_integers.append(i)

    pool.complete()
    thread.join()

    self.assertEqual(inserted_integers, [0, 1, 2, 3, 4])

  def _to_interrupt(self, pool: TasksPool[int]):
    time.sleep(0.2)
    pool.interrupt()

  def _can_only_be_interrupted(self, integer: int):
    if integer >= 3:
      while True:
        time.sleep(0.025)
        try:
          assert_continue()
        except InterruptException:
          break