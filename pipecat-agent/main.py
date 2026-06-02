import os
import asyncio
from dotenv import load_dotenv

from fastapi import FastAPI
import uvicorn

from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.serializers.exotel import ExotelFrameSerializer
from pipecat.frames.frames import AudioRawFrame, EndTaskFrame
from pipecat.processors.frame_processor import FrameDirection

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask

from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.groq.llm import GroqLLMService
from groq import AsyncGroq
from pipecat.services.deepgram.tts import DeepgramTTSService

from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.base_transport import TransportParams

from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams
)
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams

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


from twilio.rest import Client

from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams
)

import requests
from requests.auth import HTTPBasicAuth



load_dotenv()

app = FastAPI()

conversation_history = {}

def save_message(call_sid, role, text):
    if call_sid not in conversation_history:
        conversation_history[call_sid] = []

    conversation_history[call_sid].append(
        {
            "role": role,
            "content": text
        }
    )

@app.get("/")
async def root():
    return {"message": "Docker auto reload working"}




@app.get("/make-call")
async def make_call():

    client = Client(
        os.getenv("TWILIO_ACCOUNT_SID"),
        os.getenv("TWILIO_AUTH_TOKEN")
    )

    call = client.calls.create(
        to=os.getenv("MY_PHONE_NUMBER"),
        from_=os.getenv("TWILIO_PHONE_NUMBER"),
        url="https://liftable-actionable-joeann.ngrok-free.dev/incoming-call"
    )

    return {
        "message": "Call started",
        "call_sid": call.sid
    }


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

    conversation_history[call_sid] = []

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

    await run_twilio_bot(transport, call_sid)










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


async def transfer_call_handler(params):

    print("TRANSFER CALL FUNCTION CALLED")

    call_sid = params.arguments["call_sid"]

    support_number = os.getenv(
        "SUPPORT_AGENT_NUMBER"
    )

    client = Client(
        os.getenv("TWILIO_ACCOUNT_SID"),
        os.getenv("TWILIO_AUTH_TOKEN")
    )

    transfer_twiml = f"""
    <Response>
        <Dial>{support_number}</Dial>
    </Response>
    """

    client.calls(call_sid).update(
        twiml=transfer_twiml
    )

    await params.result_callback(
        "Okay, transferring your call to a human support agent now."
    )


async def warm_transfer_handler(params):

    print("WARM TRANSFER STARTED")

    call_sid = params.arguments["call_sid"]

    client = Client(
        os.getenv("TWILIO_ACCOUNT_SID"),
        os.getenv("TWILIO_AUTH_TOKEN")
    )

    conference_name = f"support-{call_sid}"

    # Get conversation history
    history = conversation_history.get(
        call_sid,
        []
    )

    history_text = "\n".join([
        f"{msg['role']}: {msg['content']}"
        for msg in history[-10:]
    ])

    # Ask LLM to summarize
    groq_client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))
    summary_response = await groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": """
                Summarize the customer's issue
                in one short sentence for a
                human support agent.
                """
            },
            {
                "role": "user",
                "content": history_text
            }
        ]
    )

    summary = (
        summary_response
        .choices[0]
        .message.content
    )

    print("SUMMARY:", summary)

    # Call support first
    client.calls.create(
        to=os.getenv("SUPPORT_AGENT_NUMBER"),
        from_=os.getenv("TWILIO_PHONE_NUMBER"),
        twiml=f"""
        <Response>
            <Say>
                Customer summary:
                {summary}
            </Say>

            <Dial>
                <Conference
                    startConferenceOnEnter="true"
                    endConferenceOnExit="false">
                    {conference_name}
                </Conference>
            </Dial>
        </Response>
        """
    )

    # Move customer into conference
    client.calls(call_sid).update(
        twiml=f"""
        <Response>
            <Say>
                Connecting you to support.
            </Say>

            <Dial>
                <Conference
                    startConferenceOnEnter="true"
                    endConferenceOnExit="true">
                    {conference_name}
                </Conference>
            </Dial>
        </Response>
        """
    )

    await params.result_callback(
        "Connecting you to support now."
    )

    


async def hangup_call_handler(params):
    print("HANGUP CALL FUNCTION CALLED")
    call_sid = params.arguments["call_sid"]
    await params.result_callback("Goodbye! Thank you for calling.")
    await asyncio.sleep(4)
    client = Client(
        os.getenv("TWILIO_ACCOUNT_SID"),
        os.getenv("TWILIO_AUTH_TOKEN")
    )
    try:
        client.calls(call_sid).update(status="completed")
    except Exception as e:
        print("Error hanging up call:", e)


async def run_twilio_bot(transport, call_sid):

    print("TWILIO BOT STARTED")

    # Voice Activity Detection
    vad_analyzer = SileroVADAnalyzer(
        params=VADParams(
            confidence=0.7,
            start_secs=0.2,
            stop_secs=0.7
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

    llm.register_function(
    "transfer_call",
    transfer_call_handler
    )

    llm.register_function(
    "warm_transfer",
    warm_transfer_handler
    )

    llm.register_function(
        "hangup_call",
        hangup_call_handler
    )


    # Text-to-speech
    tts = DeepgramTTSService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        voice="aura-2-thalia-en"
    )

    messages = [
        {
            "role": "system",
            "content": f"""
                You are a helpful and professional hotel booking phone assistant.
                Speak naturally and politely like a real receptionist.

                Your goal is to help users:
                1. Search for available hotel rooms in a city (using the 'search_rooms' tool).
                2. Book a hotel room (using the 'book_room' tool).
                3. Retrieve their booking records (using the 'get_bookings' tool).

                Rules:
                - Keep your responses short, concise, and friendly.
                - Never automatically book a room without user confirmation.
                - If details for booking (customer name, check-in date, or number of nights) are missing, ask for them one by one.
                - Speak naturally for a voice-based telephone call.
                - If the user asks to talk to a human, support person, real person, manager, or says they are unhappy, immediately use the 'warm_transfer' function.
                - Do not ask unnecessary questions before transfer.
                - Politely confirm transfer.
                - If the user says goodbye, bye, or wants to end the call, immediately call the 'hangup_call' tool to disconnect the line.

                The current call_sid is:
                {call_sid}

                IMPORTANT:
                Whenever calling 'warm_transfer' or 'hangup_call',
                always include this exact call_sid.
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
            ),

            FunctionSchema(
                name="transfer_call",
                description="Transfer the current call to a human support agent",
                properties={
                    "call_sid": {
                        "type": "string",
                        "description": "Current call SID"
                    }
                },
                required=["call_sid"]
            ),

            FunctionSchema(
                name="warm_transfer",
                description="Transfer customer to support agent using conference call",
                properties={
                    "call_sid": {
                        "type": "string"
                    }
                },
                required=["call_sid"]
            ),

            FunctionSchema(
                name="hangup_call",
                description="End the call when the conversation is finished or user says goodbye",
                properties={
                    "call_sid": {
                        "type": "string"
                    }
                },
                required=["call_sid"]
            )
        ]
    )

    context.set_tools(tools)
    context.set_tool_choice("auto")

    context_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=vad_analyzer
        )
    )

    @context_aggregator.user().event_handler("on_user_turn_stopped")
    async def on_user_context(aggregator, strategy, message):
        save_message(
            call_sid,
            "user",
            message.content
        )


    @context_aggregator.assistant().event_handler("on_assistant_turn_stopped")
    async def on_assistant_context(aggregator, message):
        save_message(
            call_sid,
            "assistant",
            message.content
        )


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
    
    # Voice Activity Detection
    vad_analyzer = SileroVADAnalyzer(
        params=VADParams(
            confidence=0.7,
            start_secs=0.2,
            stop_secs=0.5
        )
    )
    

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
            - If the user says goodbye, bye, or wants to end the call, politely say goodbye and do not call any tools.

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
    context_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=vad_analyzer
        )
    )


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

    






# @app.websocket("/exotel-media")
# async def exotel_media(websocket: WebSocket):

#     await websocket.accept()

#     print("EXOTEL CONNECTED!")

#     global current_call_sid 
#     stream_sid = None
#     call_sid = None
#     account_sid = None

#     while True:
#         message = await websocket.receive_json()

#         # print("EXOTEL EVENT:", message)

#         if message.get("event") == "start":
#             start_data = message.get("start", {})

#             stream_sid = start_data.get("streamSid") or start_data.get("stream_sid") or "TEST_STREAM"
#             call_sid = start_data.get("callSid") or start_data.get("call_sid") or "TEST_CALL"
#             current_call_sid = call_sid
#             print("CURRENT CALL SID:", current_call_sid)
#             account_sid = start_data.get("accountSid") or start_data.get("account_sid") or "TEST_ACCOUNT"
#             break

#     # print("STREAM SID:", stream_sid)
#     # print("CALL SID:", call_sid)

#     transport = FastAPIWebsocketTransport(
#         websocket=websocket,
#         params=FastAPIWebsocketParams(
#             audio_in_enabled=True,
#             audio_out_enabled=True,
#             fixed_audio_packet_size=1600,
#             serializer=ExotelFrameSerializer(
#                 stream_sid=stream_sid,
#                 call_sid=call_sid
#             )
#         )
#     )

#     print("EXOTEL BOT STARTED")

#     await run_exotel_bot(transport)