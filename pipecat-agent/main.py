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
from pipecat.services.deepgram.tts import DeepgramTTSService

from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.base_transport import TransportParams

from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair

from pipecat_ai_small_webrtc_prebuilt.frontend import SmallWebRTCPrebuiltUI

from fastapi.responses import JSONResponse

from fastapi import Request

from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.audio.turn.smart_turn.base_smart_turn import SmartTurnParams

from tools import search_rooms

from database.db import get_connection

from pipecat.processors.aggregators.llm_context import ToolsSchema
from pipecat.adapters.schemas.function_schema import FunctionSchema




load_dotenv()

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Docker auto reload working"}

# Mount Pipecat prebuilt UI
app.mount("/ui", SmallWebRTCPrebuiltUI)


async def search_rooms_handler(params):
    print("FUNCTION CALLED")

    city = params.arguments["city"]
    print("CITY:", city)

    from database.db import get_connection

    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT hotel_name, room_type, price, available_rooms
        FROM hotels
        WHERE city = %s
    """

    cursor.execute(query, (city,))
    rooms = cursor.fetchall()

    cursor.close()
    conn.close()

    if not rooms:
        await params.result_callback(
            f"Sorry, I couldn't find any hotels in {city}."
        )
        return

    response = f"I found {len(rooms)} hotel options in {city}. "

    for room in rooms:
        hotel_name, room_type, price, available_rooms = room

        response += (
            f"{hotel_name} has a {room_type} room "
            f"for rupees {price}, "
            f"with {available_rooms} rooms available. "
        )

    print(response)

    await params.result_callback(response)

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

    llm.register_function(
    "search_rooms",
    search_rooms_handler
    )


    # Text-to-speech
    tts = DeepgramTTSService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        voice="aura-2-thalia-en"
    )
    
# female-EXAVITQu4vr4xnSDxMaL

    messages = [
        {
            "role": "system",
            "content": """
            You are a hotel booking voice assistant.

            You have access to a function:

            search_rooms(city)

            IMPORTANT:
            If the user asks for hotels,
            hotel availability,
            or rooms in a city,
            ALWAYS call search_rooms.

            Do NOT say:
            search_rooms(city="Kochi")

            Actually use the function.

            Reply briefly.
            """
        }
    ]

    context = LLMContext(messages)
    tools = ToolsSchema(
        standard_tools=[
            FunctionSchema(
                name="search_rooms",
                description="Search available hotel rooms in a city",
                properties={
                    "city": {
                        "type": "string",
                        "description": "Name of the city"
                    }
                },
                required=["city"]
            )
        ]
    )

    context.set_tools(tools)
    context.set_tool_choice("auto")
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

    

