import configparser
import logging
import sys

from .io import loggers  # noqa
from .io import storage
from .models.helpers import Config
from .runtime import RaftNode


logger = logging.getLogger("raft")


def main(node_id, config):
    storage_class = getattr(storage, config.storage_class)
    with RaftNode(node_id, config, storage_class) as node:
        try:
            node.run()
        except KeyboardInterrupt:
            logger.warning("\x1b[31m*--- SHUTTING DOWN ---*\x1b[0m")
            logger.warning("\x1b[31m*--- PLEASE WAIT FOR FULL STOP ---*\x1b[0m")
    sys.exit(0)


if __name__ == "__main__":  # pragma: nocover
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="Config file path", required=True)
    parser.add_argument(
        "--node-id", "-n", help="Node Id (int)", type=int, required=True
    )
    args = parser.parse_args()

    conf = configparser.ConfigParser()
    conf.read(args.config)
    config = Config(conf)
    main(args.node_id, config)
