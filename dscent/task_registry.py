from __future__ import annotations

import queue
from concurrent.futures import Future, ThreadPoolExecutor
from enum import auto, Enum
from typing import TypeVar, Hashable, Generic

from dscent.types_ import Vertex, Interaction


class TaskType(Enum):
    SEED_GENERATION = auto()
    EXPLORATION = auto()


K = TypeVar('K', bound=Hashable)


class BoundedThreadPoolExcecutor(ThreadPoolExecutor):
    def __init__(self, max_workers: int, queue_size: int):
        super().__init__(max_workers=max_workers)
        self._work_queue = queue.Queue(maxsize=queue_size)


class TaskRegistry(Generic[K]):
    def __init__(self, max_workers: int, queue_size: int):
        self._running: dict[tuple[TaskType, K], Future] = {}
        self._completed: dict[tuple[TaskType, K], Future] = {}
        self._pool: BoundedThreadPoolExcecutor | None = (
            BoundedThreadPoolExcecutor(max_workers=max_workers, queue_size=queue_size)
            if max_workers > 0 and queue_size > 0
            else None
        )

    @staticmethod
    def _get_tasks(
            source: dict[tuple[TaskType, K], Future],
            task_type: TaskType | list[TaskType] | None = None
    ) -> dict[tuple[TaskType, K], Future]:
        task_types = (
            [task_type]
            if task_type is not None and isinstance(task_type, TaskType)
            else task_type
        )
        return {
            key: task
            for key, task
            in list(source.items())
            if task_type is None or task_type in task_types
        }

    @staticmethod
    def _get_keyed_tasks(
            source: dict[K, Future],
            task_type: TaskType
    ):
        return {
            task_key: task
            for (source_task_type, task_key), task
            in list(source.items())
            if source_task_type == task_type
        }

    def update_running_tasks(self, task_type: TaskType | None = None) -> None:
        for key, running_task in self._get_tasks(source=self._running, task_type=task_type).items():
            if running_task.done():
                # Pop from _running_tasks, put into _completed_exploration_tasks
                done_task = self._running.pop(key, None)
                if done_task is not None:
                    self._completed[key] = done_task

    def submit(self, task_type: TaskType, task_key: K, fn, *args, persist: bool = False, **kwargs) -> None:
        key = (task_type, task_key)

        if self._pool is not None:
            if persist:
                self._running[key] = self._pool.submit(fn, *args, **kwargs)
            else:
                self._pool.submit(fn, *args, **kwargs)
        else:
            result = fn(*args, **kwargs)
            if persist:
                future = Future()
                future.set_result(result)
                self._running[key] = future

    def get_running_tasks(self, task_type: TaskType) -> dict[K, Future]:
        return self._get_keyed_tasks(self._running, task_type)

    def get_completed_tasks(self, task_type: TaskType) -> dict[K, Future]:
        return self._get_keyed_tasks(self._completed, task_type)

    def get_running_seed_generation_tasks(self, vertex: Vertex) -> dict[Interaction, Future]:
        return {
            task_key: task
            for task_key, task
            in self.get_running_tasks(task_type=TaskType.SEED_GENERATION).items()
            if vertex in (task_key.source, task_key.target)
        }

    def clear_completed_tasks(self, task_type):
        for task_key in list(self.get_completed_tasks(task_type=task_type)):
            del self._completed[(task_type, task_key)]

    def shutdown(self):
        self._pool.shutdown()
