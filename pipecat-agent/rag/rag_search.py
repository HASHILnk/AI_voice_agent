from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma


CHROMA_PATH = "rag/chroma_db"


embedding_model = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2"
)


db = Chroma(
    persist_directory=CHROMA_PATH,
    embedding_function=embedding_model
)


def search_hotel_policy(question):

    results = db.similarity_search(
        question,
        k=2
    )

    if not results:
        return "No hotel information found."

    return "\n".join([
        result.page_content
        for result in results
    ])


if __name__ == "__main__":

    while True:

        query = input(
            "\nAsk hotel question: "
        )

        answer = search_hotel_policy(
            query
        )

        print("\nRESULT:\n")
        print(answer)