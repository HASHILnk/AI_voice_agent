import os
import asyncio
from dotenv import load_dotenv

from fastapi import FastAPI
import uvicorn

from pipecat.serializers.twilio import TwilioFrameSerializer

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

from tools import search_rooms, get_bookings

from database.db import get_connection

from pipecat.processors.aggregators.llm_context import ToolsSchema
from pipecat.adapters.schemas.function_schema import FunctionSchema

from fastapi.responses import Response

from fastapi import WebSocket

from twilio.twiml.voice_response import VoiceResponse, Connect

from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams
)



load_dotenv()

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Docker auto reload working"}




@app.api_route("/incoming-call", methods=["GET", "POST"])
async def incoming_call():

    twiml = """
    <Response>
        <Connect>
            <Stream url="wss://liftable-actionable-joeann.ngrok-free.dev/twilio-media" />
        </Connect>
    </Response>
    """

    return Response(
        content=twiml,
        media_type="application/xml"
    )




@app.websocket("/twilio-media")
async def twilio_media(websocket: WebSocket):

    await websocket.accept()

    print("TWILIO CONNECTED!")

    stream_sid = None
    call_sid = None
    account_sid = None

    while True:
        message = await websocket.receive_json()

        print("TWILIO EVENT:", message)

        if message.get("event") == "start":

            start_data = message.get("start", {})

            stream_sid = start_data.get(
                "streamSid",
                "TEST_STREAM"
            )

            call_sid = start_data.get(
                "callSid",
                "TEST_CALL"
            )

            account_sid = start_data.get(
                "accountSid",
                "TEST_ACCOUNT"
            )

            break

    print("STREAM SID:", stream_sid)
    print("CALL SID:", call_sid)

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            serializer=TwilioFrameSerializer(
                stream_sid=stream_sid,
                call_sid=call_sid,
                account_sid=account_sid,
                auth_token=os.getenv("TWILIO_AUTH_TOKEN")
            )
        )
    )

    print("TWILIO BOT STARTED")

    await run_twilio_bot(transport)

# Mount Pipecat prebuilt UI
app.mount("/ui", SmallWebRTCPrebuiltUI)




async def search_rooms_handler(params):
    print("FUNCTION CALLED")

    city = params.arguments["city"].strip().title()
    valid_cities = [
        "Kochi",
        "Bangalore",
        "Munnar"
    ]

    if city not in valid_cities:
        await params.result_callback(
            f"Sorry, I couldn't find hotels in {city}. "
            f"Available cities are Kochi, Bangalore, and Munnar."
        )
        return
    print("CITY:", city)

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




async def book_room_handler(params):
    print("BOOK FUNCTION CALLED")

    hotel_name = params.arguments["hotel_name"]
    customer_name = params.arguments["customer_name"]
    check_in_date = params.arguments["check_in_date"]
    nights = params.arguments["nights"]

    conn = get_connection()
    cursor = conn.cursor()

    # Check hotel availability
    cursor.execute(
        """
        SELECT hotel_name, city, room_type
        FROM hotels
        WHERE hotel_name = %s
        AND available_rooms > 0
        LIMIT 1
        """,
        (hotel_name,)
    )

    hotel = cursor.fetchone()

    if not hotel:
        cursor.close()
        conn.close()

        await params.result_callback(
            f"Sorry, no rooms are available at {hotel_name}."
        )
        return

    hotel_name, city, room_type = hotel

    # Insert booking
    cursor.execute(
        """
        INSERT INTO bookings
        (
            customer_name,
            hotel_name,
            city,
            room_type,
            check_in_date,
            nights
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            customer_name,
            hotel_name,
            city,
            room_type,
            check_in_date,
            nights
        )
    )

    # Reduce room count
    cursor.execute(
        """
        UPDATE hotels
        SET available_rooms = available_rooms - 1
        WHERE hotel_name = %s
        """,
        (hotel_name,)
    )

    conn.commit()

    cursor.close()
    conn.close()

    await params.result_callback(
        f"{customer_name}, your {room_type} room at {hotel_name} in {city} has been booked successfully for {nights} nights starting {check_in_date}."
    )



async def get_bookings_handler(params):
    print("GET BOOKINGS FUNCTION CALLED")

    customer_name = params.arguments["customer_name"]

    bookings = get_bookings(customer_name)

    if not bookings:
        await params.result_callback(
            f"I couldn't find any bookings for {customer_name}."
        )
        return

    response = f"I found {len(bookings)} booking(s) for {customer_name}. "

    for booking in bookings:
        response += (
            f"You booked {booking['hotel_name']} "
            f"in {booking['city']} with a "
            f"{booking['room_type']} room, "
            f"check-in on {booking['check_in_date']} "
            f"for {booking['nights']} nights. "
        )

    print(response)

    await params.result_callback(response)


async def run_twilio_bot(transport):

    print("TWILIO BOT STARTED")

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

    # Register tools
    llm.register_function(
        "search_rooms",
        search_rooms_handler
    )

    llm.register_function(
        "book_room",
        book_room_handler
    )

    llm.register_function(
        "get_bookings",
        get_bookings_handler
    )

    # Text-to-speech
    tts = DeepgramTTSService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        voice="aura-2-thalia-en"
    )

    messages = [
        {
            "role": "system",
            "content": """
            You are a hotel booking phone assistant.

            Speak naturally like a real receptionist.

            Available tools:
            - search_rooms
            - book_room
            - get_bookings

            Rules:
            - Never book automatically.
            - Ask for missing details.
            - Reply briefly.
            - Speak naturally for phone calls.
            """
        }
    ]

    context = LLMContext(messages)

    tools = ToolsSchema(
        standard_tools=[
            FunctionSchema(
                name="search_rooms",
                description="Search hotel rooms",
                properties={
                    "city": {
                        "type": "string"
                    },
                    "room_type": {
                        "type": "string"
                    }
                },
                required=["city"]
            ),

            FunctionSchema(
                name="book_room",
                description="Book hotel room",
                properties={
                    "hotel_name": {
                        "type": "string"
                    },
                    "customer_name": {
                        "type": "string"
                    },
                    "check_in_date": {
                        "type": "string",
                        "description": "Check-in date in YYYY-MM-DD format"
                    },
                    "nights": {
                        "type": "integer"
                    },
                    "city": {
                        "type": "string"
                    },
                    "room_type": {
                        "type": "string"
                    }
                },
                required=[
                    "hotel_name",
                    "customer_name",
                    "check_in_date",
                    "nights"
                ]
            ),

            FunctionSchema(
                name="get_bookings",
                description="Get customer bookings",
                properties={
                    "customer_name": {
                        "type": "string"
                    }
                },
                required=["customer_name"]
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

    llm.register_function(
    "book_room",
    book_room_handler
    )

    llm.register_function(
    "get_bookings",
    get_bookings_handler
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

            Available tools:

            1. search_rooms(city, room_type)
            Use for hotel search or availability.

            2. book_room(
            hotel_name,
            customer_name,
            check_in_date,
            nights
            )

            Use ONLY when the user clearly wants to book.

            Rules:
            - NEVER book automatically after searching.
            - After hotel results, wait for user confirmation.
            - Before booking, collect missing details:
                - customer name
                - check-in date
                - nights
            - Ask follow-up questions one by one.
            - Reply briefly and naturally.

            Examples:

            User: Find hotels in Kochi
            → search_rooms()

            User: Premium hotels in Bangalore
            → search_rooms()

            User: Book Grand Palace
            Assistant: What is your name?

            User: Mohammed
            Assistant: What is your check-in date?

            User: Tomorrow
            Assistant: How many nights?

            User: 2
            → book_room()
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
                    },
                    "room_type": {
                        "type": "string",
                        "description": "Type of room like Standard, Deluxe, Premium"
                    }
                },
                required=["city"]
            ),
            FunctionSchema(
                name="book_room",
                description="Book a hotel room",
                properties={
                    "hotel_name": {
                        "type": "string",
                        "description": "Name of hotel"
                    },
                    "customer_name": {
                        "type": "string",
                        "description": "Customer full name"
                    },
                    "check_in_date": {
                        "type": "string",
                        "description": "Check-in date in YYYY-MM-DD format"
                    },
                    "nights": {
                        "type": "integer",
                        "description": "Number of nights to stay"
                    },
                    "city": {
                        "type": "string",
                        "description": "City of the hotel"
                    },
                    "room_type": {
                        "type": "string",
                        "description": "Room type (e.g. Standard, Deluxe, Premium)"
                    }
                },
                required=[
                    "hotel_name",
                    "customer_name",
                    "check_in_date",
                    "nights"
                ]
            ),
            FunctionSchema(
                name="get_bookings",
                description="Get all hotel bookings of a customer",
                properties={
                    "customer_name": {  
                        "type": "string",
                        "description": "Customer full name"
                    }
                },
                required=["customer_name"]
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

    

