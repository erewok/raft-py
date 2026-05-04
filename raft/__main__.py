import configparser

from raft.main import main
from raft.models.config import Config

if __name__ == "__main__":  # pragma: nocover
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="Config file path", required=True)
    parser.add_argument("--node-id", "-n", help="Node Id (int)", type=int, required=True)
    parser.add_argument("--runtime", "-r", help="Runtime class")
    args = parser.parse_args()

    conf = configparser.ConfigParser()
    conf.read(args.config)
    config = Config(conf)
    main(args.node_id, config, runtime=args.runtime)
