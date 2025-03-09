import os
import json
from datetime import datetime, timedelta
import asyncio
import aiohttp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any

class DataProcessor:
    def __init__(self, base_path: str = "data"):
        self.base_path = base_path
        self.api_url = "https://api.fake/data"
        os.makedirs(base_path, exist_ok=True)

    def _get_parquet_path(self, date: datetime) -> str:
        """Generate path for parquet file based on date"""
        return os.path.join(
            self.base_path,
            f"year={date.year}/month={date.month:02d}/day={date.day:02d}/file.parquet"
        )

    def get_existing_dates(self) -> List[datetime]:
        """Get list of dates for which we already have data"""
        existing_dates = []
        if os.path.exists(self.base_path):
            for year_dir in os.listdir(self.base_path):
                if not year_dir.startswith("year="):
                    continue
                year = int(year_dir.split("=")[1])
                year_path = os.path.join(self.base_path, year_dir)
                
                for month_dir in os.listdir(year_path):
                    if not month_dir.startswith("month="):
                        continue
                    month = int(month_dir.split("=")[1])
                    month_path = os.path.join(year_path, month_dir)
                    
                    for day_dir in os.listdir(month_path):
                        if not day_dir.startswith("day="):
                            continue
                        day = int(day_dir.split("=")[1])
                        existing_dates.append(datetime(year, month, day))
        
        return existing_dates

    def save_to_parquet(self, data: List[Dict[str, Any]], date: datetime) -> None:
        """Save data to parquet file"""
        if not data:
            return

        df = pd.DataFrame(data)
        
        file_path = self._get_parquet_path(date)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_parquet(file_path, engine="pyarrow", index=False)


    async def fetch_data(self, date: datetime) -> List[Dict[str, Any]]:
        """Fetch data from API for specific date"""
        async with aiohttp.ClientSession() as session:
            params = {"date": date.strftime("%Y-%m-%d")}
            async with session.get(self.api_url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                return []

    async def process_date(self, date: datetime) -> None:
        """Process data for specific date"""
        data = await self.fetch_data(date)
        # with open('test_data.json', 'r', encoding='utf-8') as file:
        #     data = json.load(file)

        if data:
            self.save_to_parquet(data, date)

    async def process_missing_dates(self, days: int = 7) -> None:
        """Process missing data for the last N days"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days-1)
        existing_dates = self.get_existing_dates()
        
        tasks = []
        current_date = start_date
        while current_date <= end_date:
            if current_date not in existing_dates:
                tasks.append(self.process_date(current_date))
            current_date += timedelta(days=1)
        
        await asyncio.gather(*tasks)

async def main():
    processor = DataProcessor()
    await processor.process_missing_dates(days=7)

if __name__ == "__main__":
    asyncio.run(main()) 