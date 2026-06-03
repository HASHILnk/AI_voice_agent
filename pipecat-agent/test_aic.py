import os
import asyncio
from dotenv import load_dotenv
load_dotenv()

# We can import aic_sdk directly and try downloading models to see what works!
try:
    import aic_sdk as aic
    print("aic_sdk imported successfully!")
except ImportError as e:
    print("Failed to import aic_sdk:", e)
    exit(1)

model_ids = [
    "quail-vf-2.0-l-16khz",
    "quail-vf-2.1-l-16khz",
    "quail-vf-l-16khz",
    "quail-s-16khz",
    "quail-l-8khz"
]

for model_id in model_ids:
    print(f"\n--- Testing model: {model_id} ---")
    try:
        # download returns model path
        path = aic.Model.download(model_id, "./test_models")
        print(f"Success! Model {model_id} downloaded to: {path}")
    except Exception as e:
        print(f"Error downloading {model_id}: {e}")
