import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

from util.neo4j import NeoDriver

from titles import load_titles_nodes, load_titles_rels

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser._action_groups.pop()
    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "--load",
        help="Load nodes or relationships",
        required=True,
    )
    args = parser.parse_args()
    print(f"\n#####\nstarting ingestion at: {datetime.now()}\n#####\n")
    # initialize driver
    driver = NeoDriver(
        os.getenv("NEO4J_URI"), os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")
    )
    if args.load == "rels":
        load_titles_rels(driver)
    if args.load == "nodes":
        load_titles_nodes(driver)
    # end pipeline
    print(f"\n#####\nending ingestion at: {datetime.now()}\n#####\n")
    driver.close()