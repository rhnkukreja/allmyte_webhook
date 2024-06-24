from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pyairtable import Api
from pydantic import BaseModel
import requests
import uvicorn

AIRTABLE_ACCESS_TOKEN = "pat3ueuOClt8qDlSk.f17a120643915f116de309d97ac9d788da4b3c12988252d82c963088187dcee5"
AIRTABLE_BASE_ID = "appEF8RRj8jUjIR23"
AIRTABLE_TABLE_NAME = "sample_table"

# MongoDB connection details
MONGO_URI ="mongodb://localhost:27017/"
DATABASE_NAME = "allmyte"
COLLECTION_NAME = "allmyte_collection_1"

class RecordID(BaseModel):
    record_id: str


def get_airtable_record(record_id):
    api = Api(AIRTABLE_ACCESS_TOKEN)
    base = api.base(AIRTABLE_BASE_ID)
    table = base.table(AIRTABLE_TABLE_NAME)
    record = table.get(record_id)
    return record


def ingest_data_into_mongo(data_json):
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    try:
        collection.insert_one(data_json)
        print(f"Data inserted into MongoDB")
    except Exception as e:
        print(f"An error occurred: {e}")

# record_id="reclXIWhlYO8dV6Ah"
# data_from_airtbale=get_airtable_record(record_id)
# ingest_data_into_mongo(data_from_airtbale)

app=FastAPI()


@app.post("/ingest")
async def ingest_record(record_id: RecordID):
    try:
        # Fetch data from Airtable
        data_from_airtable = get_airtable_record(record_id.record_id)
        if not data_from_airtable:
            raise HTTPException(status_code=404, detail="Record not found in Airtable")
        
        # Ingest data into MongoDB
        ingest_data_into_mongo(data_from_airtable['fields'])
        
        return {"message": "Record ingested successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# To run the server locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)