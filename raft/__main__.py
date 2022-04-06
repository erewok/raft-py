import configparser
import logging
import sys

from raft import runtimes

from .io import loggers  # noqa
from .io import storage
from .models.config import Config


logger = logging.getLogger("raft")


def get_storage_class(runtime, storage_class):
    """Ensures the runtime requested is compatible with the
    storage class selected
    """
    if not hasattr(storage, storage_class):
        raise ValueError(f"Invalid storage class: {storage_class}")
    storage_class = getattr(storage, storage_class)
    if (
        runtime == runtimes.ThreadedRuntime
        and storage_class == storage.AsyncFileStorage
    ):
        raise ValueError(
            "Incompatible storage class: AsyncFileStorage with ThreadedRuntime"
        )
    if runtime == runtimes.AsyncRuntime and storage_class == storage.FileStorage:
        raise ValueError(
            "Incompatible storage class: AsyncRuntime with Sync FileStorage"
        )
    return storage_class


def get_runtime(runtime):
    """Ensures a valid runtime arg has been passed"""
    if isinstance(runtime, str) and hasattr(runtimes, runtime):
        return getattr(runtimes, runtime)
    elif isinstance(runtime, str) and not hasattr(runtimes, runtime):
        raise ValueError(f"Invalid runtime class: {runtime}")
    elif issubclass(runtime, runtimes.base.BaseRuntime):
        return runtime

    raise ValueError(f"Invalid runtime class: {runtime}")


def main(node_id, config, runtime="ThreadedRuntime"):
    runtime = runtime or runtimes.ThreadedRuntime
    RunClass = get_runtime(runtime)
    storage_class = get_storage_class(RunClass, config.storage_class)
    node = RunClass(node_id, config, storage_class)

    try:
        node.run()
    except KeyboardInterrupt:
        logger.warning("SHUTTING DOWN")
        logger.warning("PLEASE WAIT FOR FULL STOP")
        node.stop()
    sys.exit(0)


if __name__ == "__main__":  # pragma: nocover
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="Config file path", required=True)
    parser.add_argument(
        "--node-id", "-n", help="Node Id (int)", type=int, required=True
    )
    parser.add_argument("--runtime", "-r", help="Runtime class")
    args = parser.parse_args()

    conf = configparser.ConfigParser()
    conf.read(args.config)
    config = Config(conf)
    main(args.node_id, config, runtime=args.runtime)
