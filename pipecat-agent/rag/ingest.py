import os
import shutil

from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter

from langchain_huggingface import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma


# Folder containing PDFs
PDF_FOLDER = "rag/hotel_docs"

# Chroma database location
CHROMA_PATH = "rag/chroma_db"


def load_documents():

    documents = []

    for file in os.listdir(PDF_FOLDER):

        if file.endswith(".pdf"):

            pdf_path = os.path.join(
                PDF_FOLDER,
                file
            )

            print(f"Loading {file}")

            loader = PyPDFLoader(pdf_path)

            docs = loader.load()

            documents.extend(docs)

    return documents


def split_documents(documents):

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=100,
        chunk_overlap=20,
        separators=[
            "\n\n",
            "\n",
            ". ",
            "•",
            " "
        ]
    )

    chunks = splitter.split_documents(
        documents
    )

    return chunks


def create_vector_database(chunks):

    if os.path.exists(CHROMA_PATH):
        print(f"Clearing existing vector store at {CHROMA_PATH}...")
        shutil.rmtree(CHROMA_PATH)

    embedding_model = HuggingFaceEmbeddings(
        model_name="BAAI/bge-small-en-v1.5"
    )

    db = Chroma.from_documents(
        documents=chunks,
        embedding=embedding_model,
        persist_directory=CHROMA_PATH
    )

    print("ChromaDB created successfully")


def main():

    print("Loading PDFs...")

    docs = load_documents()

    print(f"Loaded {len(docs)} pages")

    print("Splitting into chunks...")

    chunks = split_documents(docs)

    print(f"Created {len(chunks)} chunks")

    print("Creating vector database...")

    create_vector_database(chunks)

    print("Done!")


if __name__ == "__main__":
    main()