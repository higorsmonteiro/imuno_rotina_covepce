import argparse
import yaml
from linkage import linkage

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help="Relative path, filename included, to the YAML configuration file.", type=str, required=True)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    config_path = args.config

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    linkage(config)