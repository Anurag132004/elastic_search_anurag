import json
import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pymongo import MongoClient

# ========================= Elasticsearch Setup (No Authentication) =========================
es = Elasticsearch(
    "http://localhost:9200",
    verify_certs=False,
    connections_per_node=25
)

index_name = "laws_index"

# ========================= MongoDB Atlas Setup =========================
mongo_client = MongoClient("mongodb+srv://vilsium28:LitigateIQ_db@cluster0.gz8u1.mongodb.net/")
mongo_db = mongo_client["LitigateIQ"]
mongo_collection = mongo_db["laws_index"]

# ========================= Define Index Mapping =========================
mapping = {
    "mappings": {
        "properties": {
            "chapter": {"type": "text"},
            "section_title": {"type": "text"},
            "section_content": {"type": "text"}
        }
    }
}

# ========================= Indexing Logic (Handles Your JSON Format) =========================
def generate_actions(documents):
    """Generate actions for bulk indexing in Elasticsearch."""
    for chapter_name, sections in documents.items():
        if isinstance(sections, dict):  # Ensure the value is a dictionary
            for section_title, section_content in sections.items():
                doc = {
                    "chapter": chapter_name,
                    "section_title": section_title,
                    "section_content": section_content
                }
                yield {
                    "_index": index_name,
                    "_source": doc
                }

try:
    # ========================= 1️⃣ Delete Pre-existing Elasticsearch Index =========================
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"✅ Deleted existing index '{index_name}'")

    # ========================= 2️⃣ Create a Fresh Elasticsearch Index =========================
    es.indices.create(index=index_name, body=mapping)
    print(f"✅ Created new index '{index_name}'")

    # ========================= 3️⃣ Read Data from JSON File =========================
    json_file_path = "C:/Users/anura/Desktop/litigate/my work/law_search/IP_India_government_resource_acts_PDF_sections.json"

    with open(json_file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if not isinstance(data, dict):  # Ensure the JSON file is properly structured
                raise ValueError("Invalid JSON format. Expected a dictionary with chapters as keys.")
        except json.JSONDecodeError as e:
            print(f"❌ Error reading JSON file: {e}")
            data = {}

    # ========================= 4️⃣ Bulk Index Sections in Elasticsearch =========================
    success, failed = bulk(
        es,
        generate_actions(data),
        chunk_size=500,
        raise_on_error=False
    )

    print(f"✅ Successfully indexed {success} sections")
    if failed:
        print(f"❌ Failed to index {len(failed)} sections")

    # ========================= 5️⃣ Store Sections in MongoDB Atlas (Avoid Duplicates) =========================
    for chapter_name, sections in data.items():
        if isinstance(sections, dict):
            for section_title, section_content in sections.items():
                existing_doc = mongo_collection.find_one(
                    {"chapter": chapter_name, "section_title": section_title}
                )

                if existing_doc:
                    # Update existing document
                    mongo_collection.update_one(
                        {"_id": existing_doc["_id"]},
                        {"$set": {"section_content": section_content}}
                    )
                    print(f"🔄 Updated existing document: {chapter_name} - {section_title}")
                else:
                    # Insert new document
                    mongo_collection.insert_one({
                        "chapter": chapter_name,
                        "section_title": section_title,
                        "section_content": section_content
                    })
                    print(f"✅ Inserted new document: {chapter_name} - {section_title}")

    print("✅ Indexed sections in MongoDB Atlas (without duplicates).")

    # ========================= 6️⃣ Send Data to Backend API & Print Response =========================
    response = requests.post(
        "https://litigateiq-backend.onrender.com/post/lawSearchTable",
        json=data
    )

    print(f"\n🔹 API Response Status Code: {response.status_code}")
    print(f"🔹 API Response Content: {response.text}")  # Show full API response

    if response.status_code == 200:
        print("✅ Successfully sent indexed data to backend API.")
    else:
        print("❌ API request failed. Check the response above.")

except Exception as e:
    print(f"❌ An error occurred during indexing: {e}")
