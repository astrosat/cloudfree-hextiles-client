#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import aiohttp
import asyncio
import h3
import logging
import io
import typer

from typing import Coroutine, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HEXTILE_API = "https://u41e6oc9jc.execute-api.eu-central-1.amazonaws.com/products/sentinel2-cloudfree-hex6"

MUSSELBURGH = "861972387ffffff"


def hex6_url(tile_id: str, year: int, month: int) -> str:
    return HEXTILE_API + f"?hextile={tile_id}&image=TCI&crs=epsg:3857&year={year}&month={month}"

# Only allow N simultaneous connections
conn_limit_sem = asyncio.Semaphore(10)

async def download_wait_retry(session: aiohttp.ClientSession, url: str, tile_id: str, retries: int = 5, waittime: int = 60) -> Tuple[str, bytes]:
    async with conn_limit_sem:
        logger.info(f"Requesting {tile_id} ({retries})")
        async with session.get(url) as response:
            logger.debug(f"{retries} Url: {url}")
            logger.debug(f"{retries} Status: {response.status}")
            logger.debug(f"{retries} Content-type: {response.headers['content-type']}")

    if response.status == 503 and retries > 0:
        # Wait 60 seconds and try again
        await asyncio.sleep(60)
        return await download_wait_retry(session, url, tile_id, retries=retries-1, waittime=waittime*2)


    elif response.status == 200:
        return (tile_id, await response.content.read())

    return tile_id, None



async def main(start_id: str = MUSSELBURGH, distance: int = 1, year: int = 2020, month: int = 7):

    tile_ids: List[str] = h3.k_ring(start_id, distance)
    logger.info(f"Tiles in area: {len(tile_ids)}")

    dl_tile_ids: List[str] = []
    for tid in tile_ids:
        if not Path(f"{tid}.tif").exists():
            dl_tile_ids.append(tid)
        else:
            logger.debug(f"Skip {tid} already downloaded")

    logger.info(f"Tiles to download: {len(dl_tile_ids)}")

    dl_tile_urls = [ (tid, hex6_url(tid, year, month)) for tid in dl_tile_ids ]

    async with aiohttp.ClientSession() as session:
        tasks: List[Coroutine[None, None, Tuple[str, Optional[bytes]]]] = list([ download_wait_retry(session, url, tid) for (tid, url) in dl_tile_urls ])

        for result in asyncio.as_completed(tasks):
            try:
                (tile_id, image_bytes) = await result

                if image_bytes is not None:
                    with io.open(f"{tile_id}.tif", 'wb') as f:
                        f.write(image_bytes)

                    logger.info(f"Got {tile_id} {len(image_bytes)}")

                else:
                    logger.warn(f"Failed {tile_id}")
            except:
                logger.warn(f"Error?")
                

def main_(start_id: str = MUSSELBURGH, distance: int = 1, year: int = 2020, month: int = 7, verbose: bool = False):
    logger.info(f"{start_id=} {distance=} {year=} {month=}")

    if verbose:
        logger.setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(start_id=start_id, distance=distance, year=year, month=month))

if __name__ == "__main__":
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    typer.run(main_)
