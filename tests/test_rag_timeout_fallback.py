import requests

import gocllm3


class FakeRagClient:
    def __init__(self):
        self.calls = []

    def retrieve(self, *, index_name, query_text, mode, num_result_doc, permission_groups, filter, bm25_boost, knn_boost):
        self.calls.append({
            "index_name": index_name,
            "query_text": query_text,
            "mode": mode,
            "num_result_doc": num_result_doc,
        })
        if index_name == "idx-a" and num_result_doc == 4:
            return {"hits": {"hits": []}}
        if index_name == "idx-b" and num_result_doc == 4:
            raise requests.exceptions.ReadTimeout("read timed out")
        if index_name == "idx-a" and num_result_doc == 2:
            return {
                "hits": {
                    "hits": [
                        {
                            "_score": 1.23,
                            "_source": {"title": "fallback doc"},
                        }
                    ]
                }
            }
        raise AssertionError(f"unexpected call: index={index_name}, top_k={num_result_doc}")


def test_search_rag_documents_retries_with_single_index_on_read_timeout(monkeypatch):
    fake_client = FakeRagClient()
    monkeypatch.setattr(gocllm3, "create_rag_client", lambda: fake_client)

    result = gocllm3.search_rag_documents(
        "지연 테스트",
        indexes=["idx-a", "idx-b"],
        top_k=4,
        mode="hybrid",
        return_meta=True,
    )

    docs = result["documents"]
    metadata = result["metadata"]

    assert [call["index_name"] for call in fake_client.calls] == ["idx-a", "idx-b", "idx-a"]
    assert [call["num_result_doc"] for call in fake_client.calls] == [4, 4, 2]
    assert len(docs) == 1
    assert docs[0]["title"] == "fallback doc"
    assert docs[0]["_index"] == "idx-a"
    assert metadata["timeout_occurred"] is True
    assert metadata["fallback_used"] is True
    assert metadata["effective_indexes"] == ["idx-a"]
    assert metadata["effective_top_k"] == 2
    assert metadata["user_notice"] == "문서 검색 응답이 지연되어 범위를 줄여 재조회했습니다."


def test_retrieve_rag_documents_parallel_aggregates_timeout_metadata(monkeypatch):
    def fake_search(query, indexes=None, top_k=None, mode=None, filter=None, return_meta=False):
        assert return_meta is True
        return {
            "documents": [{"title": query, "_index": "idx-a"}],
            "metadata": {
                "query": query,
                "timeout_occurred": query == "q1",
                "fallback_used": query == "q1",
                "user_notice": "문서 검색 응답이 지연되어 범위를 줄여 재조회했습니다." if query == "q1" else "",
            },
        }

    monkeypatch.setattr(gocllm3, "search_rag_documents", fake_search)

    result = gocllm3.retrieve_rag_documents_parallel(["q1", "q2"], top_k=3, indexes=["idx-a"], return_meta=True)

    assert len(result["documents"]) == 2
    assert result["metadata"]["timeout_occurred"] is True
    assert result["metadata"]["fallback_used"] is True
    assert result["metadata"]["user_notice"] == "문서 검색 응답이 지연되어 범위를 줄여 재조회했습니다."
    assert len(result["metadata"]["queries"]) == 2
