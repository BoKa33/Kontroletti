import httpx
import gtfs_kit as gk
import pandas as pd
from sqlalchemy import insert
from ..models import Base, Agency, Route, Stop, Trip, StopTime
from ..database import engine, async_session
import tempfile
import os

GTFS_URL = "https://gtfs.de/latest/gtfs.zip"

async def download_gtfs():
    async with httpx.AsyncClient() as client:
        response = await client.get(GTFS_URL)
        if response.status_code == 200:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                tmp.write(response.content)
                return tmp.name
    return None

async def import_gtfs():
    # 1. Download
    filepath = await download_gtfs()
    if not filepath:
        print("Failed to download GTFS")
        return

    # 2. Parse using gtfs-kit
    feed = gk.read_feed(filepath, dist_units='km')
    
    # 3. Import to DB
    async with async_session() as session:
        # Agency
        if not feed.agency.empty:
            agencies = feed.agency[['agency_id', 'agency_name', 'agency_url', 'agency_timezone']].rename(columns={
                'agency_id': 'id', 'agency_name': 'name', 'agency_url': 'url', 'agency_timezone': 'timezone'
            }).to_dict(orient='records')
            await session.execute(insert(Agency).values(agencies))

        # Route
        if not feed.routes.empty:
            routes = feed.routes[['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']].rename(columns={
                'route_id': 'id', 'route_short_name': 'short_name', 'route_long_name': 'long_name', 'route_type': 'type'
            }).to_dict(orient='records')
            await session.execute(insert(Route).values(routes))

        # Stop (Spatial Data)
        if not feed.stops.empty:
            # We need to create a Point(lon lat) for GeoAlchemy
            for _, row in feed.stops.iterrows():
                stop = Stop(
                    id=row['stop_id'],
                    name=row['stop_name'],
                    location=f"POINT({row['stop_lon']} {row['stop_lat']})",
                    platform_code=row.get('platform_code')
                )
                session.add(stop)

        # Trip
        if not feed.trips.empty:
            trips = feed.trips[['trip_id', 'route_id', 'trip_headsign', 'direction_id', 'shape_id']].rename(columns={
                'trip_id': 'id', 'trip_headsign': 'headsign', 'direction_id': 'direction_id', 'shape_id': 'shape_id'
            }).to_dict(orient='records')
            await session.execute(insert(Trip).values(trips))

        # StopTime (Large Data - Bulk Insert)
        if not feed.stop_times.empty:
            # Process in chunks to avoid memory issues
            chunk_size = 10000
            for i in range(0, len(feed.stop_times), chunk_size):
                chunk = feed.stop_times.iloc[i:i+chunk_size]
                st_data = chunk[['trip_id', 'stop_id', 'arrival_time', 'departure_time', 'stop_sequence']].rename(columns={
                    'arrival_time': 'arrival_time', 'departure_time': 'departure_time', 'stop_sequence': 'stop_sequence'
                }).to_dict(orient='records')
                await session.execute(insert(StopTime).values(st_data))

        await session.commit()
    
    # Cleanup
    os.remove(filepath)
    print("GTFS Static Import Complete")

if __name__ == "__main__":
    import asyncio
    asyncio.run(import_gtfs())
