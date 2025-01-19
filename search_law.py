import json
from elasticsearch import Elasticsearch

# ========================= Elasticsearch Setup (No Authentication) =========================
es = Elasticsearch(
    "http://localhost:9200",  # No authentication required
    verify_certs=False,
    connections_per_node=25  # Optimized connection pool
)

index_name = "laws_index"


def search_laws(query):
    """
    Search for legal documents in Elasticsearch.
    Returns the top 5 most relevant results.
    """
    try:
        # Construct search query
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "chapter^3",  # Boost chapter relevance
                                    "section_title^2",  # Boost section title relevance
                                    "section_content"
                                ],
                                "type": "phrase_prefix",  # Allow partial matches
                                "slop": 2  # Allow small word reordering
                            }
                        },
                        {
                            "match": {
                                "combined_text": {
                                    "query": query,
                                    "operator": "and",  # All terms must match
                                    "fuzziness": "AUTO"  # Allow typo corrections
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "chapter": {},
                    "section_title": {},
                    "section_content": {}
                },
                "pre_tags": ["<strong>"],  # Highlight match
                "post_tags": ["</strong>"]
            },
            "size": 5  # Return top 5 results
        }

        response = es.search(index=index_name, body=search_body)

        if response["hits"]["total"]["value"] > 0:
            results = []
            for hit in response["hits"]["hits"]:
                result = hit["_source"]
                result["score"] = hit["_score"]
                result["highlights"] = hit.get("highlight", {})
                results.append(result)
            return results
        return None

    except Exception as e:
        print(f"❌ An error occurred during search: {e}")
        return None


def main():
    print("📜 Legal Document Search System 📜")
    print("🔎 Enter your search terms or type 'exit' to quit")

    while True:
        query = input("\nSearch query: ").strip()
        if query.lower() == "exit":
            print("👋 Exiting. Goodbye!")
            break

        results = search_laws(query)
        if results:
            print("\n🔹 Search Results:")
            for i, result in enumerate(results, start=1):
                print(f"\n📖 **Result {i}:**")
                print(f"🔹 **Chapter:** {result.get('chapter', 'N/A')}")
                print(f"🔹 **Section Title:** {result.get('section_title', 'N/A')}")
                print(f"🔹 **Content:** {result.get('section_content', 'N/A')}")
                print(f"🔹 **Relevance Score:** {result.get('score'):.2f}")

                # Display highlights if available
                if "highlights" in result:
                    print("🔹 **Highlighted Matches:**")
                    print(json.dumps(result["highlights"], indent=4, ensure_ascii=False))
        else:
            print("\n❌ No results found. Try a different search term.")


if __name__ == "__main__":
    main()
