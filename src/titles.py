import os
import time
import datetime
import pandas as pd

write_titles = """
    CALL apoc.periodic.iterate(
        "UNWIND $titles AS title RETURN title",
        "MERGE (t:Title {tconst: title.tconst})
        ON CREATE SET t += title",
        {batchSize: $batchsize, parallel: true, concurrency: $concurrency, params: {titles: $titles}}
    )
"""

write_genres = """
    CALL apoc.periodic.iterate(
        "UNWIND $titles AS title RETURN title",
        "WITH DISTINCT title.genres AS genres
        WITH split(genres, ',') AS genresList
        UNWIND genresList AS genre
        MERGE (:Genre {name: genre})",
        {batchSize: $batchsize, parallel: false, params: {titles: $titles}}
    )
"""

connect_titles_to_genres = """
    CALL apoc.periodic.iterate(
        "UNWIND $titles AS title RETURN title",
        "WITH title.tconst AS tconst, title.genres AS genres
        WITH tconst, split(genres, ',') AS genresList
        UNWIND genresList AS genre
        MATCH (g:Genre {name: genre}), (t:Title {tconst: tconst})
        MERGE (t)-[:HAS_GENRE]->(g)",
        {batchSize: $batchsize, parallel: false, params: {titles: $titles}}
    )
"""

def load_titles_nodes(driver):
    processed = 0
    chunksize = int(os.getenv("CSV_CHUNK_SIZE"))
    filename = "./data/" + os.getenv("FILE_NAME_TITLES")
    batchsize = int(os.getenv("NEO4J_BATCH_SIZE"))
    concurrency = int(os.getenv("NEO4J_BATCH_CONCURRENCY"))
    start = time.perf_counter()
    # chunk csv file to keep memory usage linear
    for chunk in pd.read_csv(
        filename,
        sep='\t',
        dtype=str,
        low_memory=False,
        chunksize=chunksize,
        skipinitialspace=True,
        error_bad_lines=False,
    ):
        titles = chunk.to_dict("records")
        # write to neo4j
        driver.write(write_genres, titles=titles, batchsize=batchsize)
        driver.write(write_titles, titles=titles, batchsize=batchsize, concurrency=concurrency)
        # log progress
        processed = processed + 1
        if processed % 10 == 0:
            end = time.perf_counter()
            print(
                "Loaded "
                + str(processed)
                + f" chunks of size {chunksize} in total."
                + f" Loaded the current chunk in {end - start:0.4f} seconds."
            )
            if (processed == 50):
                break
            start = time.perf_counter()

def load_titles_rels(driver):
    processed = 0
    chunksize = int(os.getenv("CSV_CHUNK_SIZE"))
    filename = "./data/" + os.getenv("FILE_NAME_TITLES")
    batchsize = int(os.getenv("NEO4J_BATCH_SIZE"))
    concurrency = int(os.getenv("NEO4J_BATCH_CONCURRENCY"))
    start = time.perf_counter()
    # chunk csv file to keep memory usage linear
    for chunk in pd.read_csv(
        filename,
        sep='\t',
        dtype=str,
        low_memory=False,
        chunksize=chunksize,
        skipinitialspace=True,
        error_bad_lines=False,
    ):
        titles = chunk.to_dict("records")
        # write to neo4j
        driver.write(connect_titles_to_genres, titles=titles, batchsize=batchsize)
        # log progress
        processed = processed + 1
        if processed % 10 == 0:
            end = time.perf_counter()
            print(
                "Loaded "
                + str(processed)
                + f" chunks of size {chunksize} in total."
                + f" Loaded the current chunk in {end - start:0.4f} seconds."
            )
            if (processed == 50):
                break
            start = time.perf_counter()