#!/usr/bin/env python
# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import h3
import logging
import io
import typer

from typing import Coroutine, List, Optional, Tuple

logger = logging.getLogger()

HEXTILE_API = "https://u41e6oc9jc.execute-api.eu-central-1.amazonaws.com/products/sentinel2-cloudfree-hex6"

MUSSELBURGH = "861972387ffffff"


def hex6_url(tile_id: str, year: int, month: int) -> str:
    return HEXTILE_API + f"?hextile={tile_id}&image=TCI&crs=epsg:3857&year={year}&month={month}"


async def download_wait_retry(session: aiohttp.ClientSession, url: str, tile_id: str) -> Tuple[str, bytes]:
        async with session.get(url) as response:
            print("1 Url: ", url)
            print("1 Status:", response.status)
            print("1 Content-type:", response.headers['content-type'])

            if response.status == 503:
                # Wait 60 seconds and try again
                await asyncio.sleep(60)

                async with session.get(url) as response:
                    print("2 Url: ", url)
                    print("2 Status:", response.status)
                    print("2 Content-type:", response.headers['content-type'])

                    if response.status == 200:
                        return await response.content.read()

            elif response.status == 200:
                return (tile_id, await response.content.read())

        return tile_id, None



async def main(start_id: str = MUSSELBURGH, distance: int = 1, year: int = 2020, month: int = 7):

    tile_ids: List[str] = h3.k_ring(start_id, distance)
    print(f"Tiles to download: {tile_ids}")

    tile_urls = [ (tid, hex6_url(tid, year, month)) for tid in tile_ids ]
    #print(f"{tile_urls=}")

    async with aiohttp.ClientSession() as session:
        tasks: List[Coroutine[None, None, Tuple[str, Optional[bytes]]]] = list([ download_wait_retry(session, url, tid) for (tid, url) in tile_urls ])

        for result in asyncio.as_completed(tasks):
            try:
                (tile_id, image_bytes) = await result

                if image_bytes is not None:
                    with io.open(f"{tile_id}.tif", 'wb') as f:
                        f.write(image_bytes)

                    print(f"Got {tile_id} {len(image_bytes)}")

                else:
                    print(f"Failed {tile_id}")
            except:
                print(f"Error?")
                

def main_(start_id: str = MUSSELBURGH, distance: int = 1, year: int = 2020, month: int = 7):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(start_id=start_id, distance=distance, year=year, month=month))

if __name__ == "__main__":
    typer.run(main_)
