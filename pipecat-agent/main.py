import os
import asyncio
from dotenv import load_dotenv

from fastapi import FastAPI
import uvicorn

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask

from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.groq.llm import GroqLLMService
from pipecat.services.elevenlabs.tts import ElevenLabsTTSService

from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.base_transport import TransportParams

from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair

from pipecat_ai_small_webrtc_prebuilt.frontend import SmallWebRTCPrebuiltUI

from fastapi.responses import JSONResponse

from fastapi import Request


load_dotenv()

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Docker auto reload working"}

# Mount Pipecat prebuilt UI
app.mount("/ui", SmallWebRTCPrebuiltUI)



async def run_bot(webrtc_connection):
    
    # WebRTC Transport
    
    transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
        )
    )



    # Speech-to-text
    stt = DeepgramSTTService(
        api_key=os.getenv("DEEPGRAM_API_KEY")
    )

    # LLM
    llm = GroqLLMService(
        api_key=os.getenv("GROQ_API_KEY"),
        settings=GroqLLMService.Settings(
            model="llama-3.3-70b-versatile"
        )
    )

    # Text-to-speech
    tts = ElevenLabsTTSService(
        api_key=os.getenv("ELEVENLABS_API_KEY"),
        settings=ElevenLabsTTSService.Settings(voice="pNInz6obpgDQGcFmaJgB")
    )
    


    messages = [
        {
            "role": "system",
            "content": "You are a friendly AI voice assistant. Reply briefly."
        }
    ]

    context = LLMContext(messages)
    context_aggregator = LLMContextAggregatorPair(context)


    pipeline = Pipeline([
        transport.input(),
        stt,
        context_aggregator.user(),
        llm,
        tts,
        transport.output(),
        context_aggregator.assistant()
    ])


    task = PipelineTask(pipeline)
    runner = PipelineRunner()
    await runner.run(task)



@app.post("/start")
async def start(request: Request):

    return {
        "webrtcUrl": "/connect"
    }

@app.post("/connect")
async def connect(request: Request):
    payload = await request.json()
    print("\n====== PAYLOAD ======")
    print(payload)
    offer_sdp = payload.get("sdp")
    offer_type = payload.get("type")
    webrtc_connection = SmallWebRTCConnection()
    await webrtc_connection.initialize(
        offer_sdp,
        offer_type
    )
    asyncio.create_task(
        run_bot(webrtc_connection)
    )
    answer = webrtc_connection.get_answer()
    print("\n====== ANSWER ======")
    print(answer)
    return answer

    

