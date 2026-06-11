import os
import asyncio
from dotenv import load_dotenv
import uuid

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
from pipecat.services.sarvam.stt import SarvamSTTService
from pipecat.services.sarvam.tts import SarvamTTSService

from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.base_transport import TransportParams

from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams
)
from pipecat.audio.vad.vad_analyzer import VADParams

from pipecat_ai_small_webrtc_prebuilt.frontend import SmallWebRTCPrebuiltUI

from fastapi.responses import JSONResponse

from fastapi import Request

from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.audio.turn.smart_turn.base_smart_turn import SmartTurnParams

from tools import search_rooms, get_bookings

from database.db import get_connection
from database.init_db import initialize_database

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

from pipecat.audio.filters.aic_filter import AICFilter
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams
)

from rag.rag_search import (
    search_hotel_policy
)


load_dotenv()

app = FastAPI()

@app.on_event("startup")
def startup_event():
    initialize_database()

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

    aic_filter = AICFilter(
        license_key=os.getenv("AIC_SDK_LICENSE"),
        model_id="quail-vf-2.1-l-16khz",
        enhancement_level=0.80
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_filter=aic_filter,
            serializer=TwilioFrameSerializer(
                stream_sid=stream_sid,
                call_sid=call_sid,
                account_sid=account_sid,
                auth_token=os.getenv("TWILIO_AUTH_TOKEN")
            )
        )
    )

    print("TWILIO BOT STARTED")

    await run_twilio_bot(transport, call_sid, aic_filter)










# Mount Pipecat prebuilt UI
app.mount("/ui", SmallWebRTCPrebuiltUI)




async def search_rooms_handler(params):
    print("FUNCTION CALLED")

    try:
        city = params.arguments["city"].strip().title()

        valid_cities = [
            "Kochi",
            "Bangalore",
            "Munnar"
        ]

        if city not in valid_cities:
            await params.result_callback(
                f"Sorry, we currently support hotels only in Kochi, Bangalore, and Munnar."
            )
            return

        hotels = search_rooms(city)

        if not hotels:
            await params.result_callback(
                f"Sorry, I couldn't find hotels in {city} right now."
            )
            return

        response = f"I found some hotel options in {city}. "

        for hotel in hotels:
            if hotel["available_rooms"] > 0:
                response += (
                    f"{hotel['hotel_name']} has a "
                    f"{hotel['room_type']} room for "
                    f"{hotel['price']} rupees, "
                    f"with {hotel['available_rooms']} rooms available. "
                )

        response += (
            "Which hotel would you like?"
        )

        await params.result_callback(response)

    except Exception as e:
        print("SEARCH ERROR:", e)

        await params.result_callback(
            "Sorry, our hotel booking system is temporarily unavailable."
        )




async def book_room_handler(params):
    print("BOOK FUNCTION CALLED")

    hotel_name = params.arguments["hotel_name"]
    customer_name = params.arguments["customer_name"]
    customer_phone = params.arguments.get("customer_phone")
    check_in_date = params.arguments["check_in_date"]
    nights = params.arguments["nights"]
    city = params.arguments.get("city")
    if city:
        city = city.strip().title()
    room_type = params.arguments.get("room_type")
    if room_type:
        room_type_lower = room_type.lower()
        if "standard" in room_type_lower:
            room_type = "Standard"
        elif "deluxe" in room_type_lower:
            room_type = "Deluxe"
        elif "premium" in room_type_lower:
            room_type = "Premium"
        else:
            room_type = room_type.strip().title()

    # Format customer_phone to E.164
    if customer_phone:
        digits = "".join(c for c in str(customer_phone) if c.isdigit())
        if len(digits) == 10:
            customer_phone = f"+91{digits}"
        elif len(digits) == 12 and digits.startswith("91"):
            customer_phone = f"+{digits}"
        elif not str(customer_phone).startswith("+"):
            customer_phone = f"+{digits}"

    conn = get_connection()
    cursor = conn.cursor()

    # Fallback lookups for missing arguments (city, room_type)
    if not city and hotel_name:
        try:
            cursor.execute(
                "SELECT DISTINCT city FROM hotels WHERE LOWER(hotel_name) = LOWER(%s) LIMIT 1;",
                (hotel_name,)
            )
            row = cursor.fetchone()
            if row:
                city = row[0]
                print(f"Looked up missing city: {city}")
        except Exception as e:
            print("Error looking up missing city:", e)

    if not room_type and hotel_name:
        try:
            cursor.execute(
                "SELECT DISTINCT room_type FROM hotels WHERE LOWER(hotel_name) = LOWER(%s) AND available_rooms > 0 LIMIT 1;",
                (hotel_name,)
            )
            row = cursor.fetchone()
            if row:
                room_type = row[0]
                print(f"Looked up missing room_type: {room_type}")
        except Exception as e:
            print("Error looking up missing room_type:", e)

    # Check hotel availability
    cursor.execute(
        """
        SELECT hotel_name, city, room_type
        FROM hotels
        WHERE hotel_name = %s
        AND city = %s
        AND room_type = %s
        AND available_rooms > 0
        LIMIT 1
        """,
        (hotel_name, city, room_type)
    )

    hotel = cursor.fetchone()

    if not hotel:
        cursor.close()
        conn.close()

        await params.result_callback(
            f"Sorry, no {room_type} rooms are currently available at {hotel_name}."
        )
        return

    hotel_name, city, room_type = hotel

    # Insert booking
    cursor.execute(
        """
        INSERT INTO bookings
        (
            customer_name,
            customer_phone,
            hotel_name,
            city,
            room_type,
            check_in_date,
            nights
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            customer_name,
            customer_phone,
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
        AND city = %s
        AND room_type = %s
        """,
        (hotel_name, city, room_type)
    )

    conn.commit()

    cursor.close()
    conn.close()

    # Send confirmation SMS if customer_phone is provided
    if customer_phone:
        try:
            from twilio.rest import Client
            twilio_client = Client(
                os.getenv("TWILIO_ACCOUNT_SID"),
                os.getenv("TWILIO_AUTH_TOKEN")
            )
            sms_body = f"Booking confirmed! Your {room_type} room at {hotel_name} in {city} has been booked successfully for {nights} nights starting {check_in_date}."
            
            # Send SMS
            try:
                print(f"Sending confirmation SMS to {customer_phone}...")
                twilio_client.messages.create(
                    body=sms_body,
                    from_=os.getenv("TWILIO_PHONE_NUMBER"),
                    to=customer_phone
                )
                print("SMS sent successfully.")
            except Exception as e:
                print("Error sending SMS:", e)
                
        except Exception as e:
            print("Error initializing Twilio client for notifications:", e)

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


async def hotel_policy_handler(params):

    print("HOTEL POLICY SEARCH")

    question = params.arguments[
        "question"
    ]

    result = search_hotel_policy(
        question
    )

    await params.result_callback(
        result
    )


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


async def run_twilio_bot(transport, call_sid, aic_filter):
    print("TWILIO BOT STARTED")

    # Speech-to-text
    stt = SarvamSTTService(
        api_key=os.getenv("SARVAM_API_KEY"),
        settings=SarvamSTTService.Settings(
            model="saaras:v3",
            language="unknown"
        )
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
        "search_hotel_policy",
        hotel_policy_handler
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
    tts = SarvamTTSService(
        api_key=os.getenv("SARVAM_API_KEY"),
        settings=SarvamTTSService.Settings(
            model="bulbul:v3-beta",
            voice="shubh"
        )
    )

    messages = [
        {
            "role": "system",
            "content": f"""
            You are a warm, friendly, professional hotel receptionist speaking over a phone call.

            Speak naturally like a real human receptionist, not like an AI assistant.

            Your job:
            - Help customers find hotels
            - Book hotel rooms
            - Retrieve booking details
            - Answer hotel policy questions
            - Transfer to a human if needed

            Conversation Style & Language Examples:
            - Keep responses short and natural.
            - Sound warm, polite, and conversational.
            - Never sound robotic.
            - Speak in the SAME language as the user.
            - Support English, Malayalam, Hindi, Tamil, and mixed-language speech.
            - If the customer mixes languages, respond naturally in the same mixed style.
            - Never force English.
            - If the user asks to speak in another language, switch naturally and continue in that language.

            Mixed-Language Examples:
            User: "Kochi-yil room undo?"
            Assistant: "Yes, Kochi-il rooms available undu. What type of room are you looking for?"

            User: "Munnar me hotel available hai?"
            Assistant: "Yes, Munnar mein hotels available hain. Aapko standard ya deluxe room chahiye?"

            User: "Can you speak Malayalam?"
            Assistant: "ശരി, മലയാളത്തിൽ സംസാരിക്കാം. എന്ത് സഹായമാണ് വേണ്ടത്?"

            Greetings & Filler Words:
            - For greetings or filler words like: "hi", "hello", "okay", "yes", "hmm", "alright" or short acknowledgements, respond naturally and briefly.
            Examples:
            User: "Hi"
            Assistant: "Hello! How can I help you?"
            User: "Okay"
            Assistant: "Sure 🙂"
            - Do not treat greetings or filler words as hotel search requests.
            - Do not say "I didn't catch that" for normal greetings or filler words.
            - If speech is completely unclear, has low confidence, is ambiguous, or is silent, politely ask: "Sorry, I didn't catch that. Could you repeat please?"

            Language & City Guessing Rules:
            - Never infer or guess a city from language, accent, random words, or uncertain pronunciation (e.g. if the user speaks Gujarati, do not guess Ahmedabad).
            - Only search for hotels after the user clearly mentions a city name.
            - If uncertain or if the city has not been specified, politely ask: "Sorry, which city are you looking for?"

            Hotel Search Flow & Rules:
            - Step 1: When the user mentions a city (e.g., "Kochi"), respond naturally saying you will search (e.g., "Kochi-yil hotel check cheyyam. Oru nimisham.") and immediately trigger the 'search_rooms' tool with the city name. Do NOT ask for the room type or other details before searching.
            - Step 2: Once the tool returns the list of hotels, present them to the user (e.g., "Kochi-yil 3 hotels available undu: Grand Palace, Marina Inn, Sea View Hotel. Ithil etha book cheyyendathu?") and ask which hotel they would like to select.
            - Step 3: Only after the user selects a hotel, ask for the room type.
            - Never invent hotel names, room types, prices, or availability. Only use information returned from tools.
            - If the city name sounds unclear or you are uncertain because of pronunciation, confirm: "Did you mean Kochi?" or "Could you repeat the city name please?"

            Booking Rules:
            - Never book automatically.
            - First let the customer choose a hotel.
            - Collect details ONE question at a time in this order:
              1. Customer name
              2. Customer phone number (must verify it is exactly a 10-digit number; if they provide an invalid number, ask them to repeat/verify)
              3. Check-in date
              4. Number of nights
            - Never ask multiple questions together.
            - Never guess missing details.
            - Before calling 'book_room', confirm details briefly. Example: "Just confirming — Grand Palace in Kochi, for John Doe at 9847573743, check-in on December 5, for 2 nights. Shall I book it?"
            - Only call 'book_room' after the customer clearly confirms (e.g., "yes", "okay", "correct", "book it").

            Hotel Policies:
            - If the customer asks about: pets, breakfast, wifi, parking, cancellation, check-in/out, pool, gym, pickup, smoking, room service, late checkout, ID proof, or hotel rules, ALWAYS use the 'search_hotel_policy' tool before answering. Never guess.

            Human Transfer:
            - Call the 'warm_transfer' tool only if the customer explicitly asks for: human, manager, support, real person, transfer call, OR if the customer is frustrated after repeated failures.
            - Do not transfer for greetings, silence, or language switching.
            - Politely confirm transfer and call the tool immediately.

            Call Ending:
            - If the customer says goodbye, bye, or wants to end the call, immediately call the 'hangup_call' tool to disconnect.

            Tool Calling Formatting Rules:
            - You must call tools natively using the function calling interface.
            - Never output raw JSON, XML tags, or code snippets (e.g., `<function=...>`, `</function>`, `{{"city": ...}}`) in your conversational text responses.
            - Do not describe or output the function call to the user. Execute the tool natively and speak only in normal, friendly language.

            Current call_sid:
            {call_sid}

            Always include this call_sid when calling:
            - 'warm_transfer'
            - 'hangup_call'
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
                    "customer_phone": {
                        "type": "string",
                        "description": "The customer's 10-digit phone number (e.g. 9847573743)"
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
                    "customer_phone",
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
                name="search_hotel_policy",
                description="""
                Search hotel policy, FAQs,
                amenities, cancellation,
                pet policy, timings,
                hotel services and rules.
                """,
                properties={
                    "question": {
                        "type": "string",
                        "description":
                        "Hotel-related question"
                    }
                },
                required=["question"]
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

    vad_analyzer = aic_filter.create_vad_analyzer()
    vad_analyzer._sample_rate = 16000
    vad_analyzer.set_params(
        VADParams(
            confidence=0.5,
            start_secs=0.0,
            stop_secs=0.3,
            min_volume=0.0
        )
    )

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
    call_sid = f"webrtc-{uuid.uuid4().hex[:12]}"
    conversation_history[call_sid] = []
    
    aic_filter = AICFilter(
        license_key=os.getenv("AIC_SDK_LICENSE"),
        model_id="quail-vf-2.1-l-16khz",
        enhancement_level=0.80
    )
    
    # WebRTC Transport
    transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_filter=aic_filter
        )
    )

    async def webrtc_transfer_call_handler(params):
        print("WEBRTC TRANSFER CALL FUNCTION CALLED")
        await params.result_callback(
            "Okay, transferring your call to a human support agent now. Please hold on."
        )
        await task.stop_when_done()

    async def webrtc_warm_transfer_handler(params):
        print("WEBRTC WARM TRANSFER FUNCTION CALLED")
        await params.result_callback(
            "Connecting you to support now. Please hold on."
        )
        await task.stop_when_done()

    async def webrtc_hangup_call_handler(params):
        print("WEBRTC HANGUP CALL FUNCTION CALLED")
        await params.result_callback(
            "Goodbye! Thank you for using our web assistant."
        )
        await task.stop_when_done()



    # Speech-to-text
    stt = SarvamSTTService(
        api_key=os.getenv("SARVAM_API_KEY"),
        settings=SarvamSTTService.Settings(
            model="saaras:v3"
        )
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

    llm.register_function(
        "search_hotel_policy",
        hotel_policy_handler
    )

    llm.register_function(
        "transfer_call",
        webrtc_transfer_call_handler
    )

    llm.register_function(
        "warm_transfer",
        webrtc_warm_transfer_handler
    )

    llm.register_function(
        "hangup_call",
        webrtc_hangup_call_handler
    )


    # Text-to-speech
    tts = SarvamTTSService(
        api_key=os.getenv("SARVAM_API_KEY"),
        settings=SarvamTTSService.Settings(
            model="bulbul:v3-beta",
            voice="shubh"
        )
    )
    
# female-EXAVITQu4vr4xnSDxMaL

    messages = [
        {
            "role": "system",
            "content": f"""
            You are a warm, friendly, professional hotel receptionist speaking over a phone call.

            Speak naturally like a real human receptionist, not like an AI assistant.

            Your job:
            - Help customers find hotels
            - Book hotel rooms
            - Retrieve booking details
            - Answer hotel policy questions
            - Transfer to a human if needed

            Conversation Style & Language Examples:
            - Keep responses short and natural.
            - Sound warm, polite, and conversational.
            - Never sound robotic.
            - Speak in the SAME language as the user.
            - Support English, Malayalam, Hindi, Tamil, and mixed-language speech.
            - If the customer mixes languages, respond naturally in the same mixed style.
            - Never force English.
            - If the user asks to speak in another language, switch naturally and continue in that language.

            Mixed-Language Examples:
            User: "Kochi-yil room undo?"
            Assistant: "Yes, Kochi-il rooms available undu. What type of room are you looking for?"

            User: "Munnar me hotel available hai?"
            Assistant: "Yes, Munnar mein hotels available hain. Aapko standard ya deluxe room chahiye?"

            User: "Can you speak Malayalam?"
            Assistant: "ശരി, മലയാളത്തിൽ സംസാരിക്കാം. എന്ത് സഹായമാണ് വേണ്ടത്?"

            Greetings & Filler Words:
            - For greetings or filler words like: "hi", "hello", "okay", "yes", "hmm", "alright" or short acknowledgements, respond naturally and briefly.
            Examples:
            User: "Hi"
            Assistant: "Hello! How can I help you?"
            User: "Okay"
            Assistant: "Sure 🙂"
            - Do not treat greetings or filler words as hotel search requests.
            - Do not say "I didn't catch that" for normal greetings or filler words.
            - If speech is completely unclear, has low confidence, is ambiguous, or is silent, politely ask: "Sorry, I didn't catch that. Could you repeat please?"

            Language & City Guessing Rules:
            - Never infer or guess a city from language, accent, random words, or uncertain pronunciation (e.g. if the user speaks Gujarati, do not guess Ahmedabad).
            - Only search for hotels after the user clearly mentions a city name.
            - If uncertain or if the city has not been specified, politely ask: "Sorry, which city are you looking for?"

            Hotel Search Flow & Rules:
            - Step 1: When the user mentions a city (e.g., "Kochi"), respond naturally saying you will search (e.g., "Kochi-yil hotel check cheyyam. Oru nimisham.") and immediately trigger the 'search_rooms' tool with the city name. Do NOT ask for the room type or other details before searching.
            - Step 2: Once the tool returns the list of hotels, present them to the user (e.g., "Kochi-yil 3 hotels available undu: Grand Palace, Marina Inn, Sea View Hotel. Ithil etha book cheyyendathu?") and ask which hotel they would like to select.
            - Step 3: Only after the user selects a hotel, ask for the room type.
            - Never invent hotel names, room types, prices, or availability. Only use information returned from tools.
            - If the city name sounds unclear or you are uncertain because of pronunciation, confirm: "Did you mean Kochi?" or "Could you repeat the city name please?"

            Booking Rules:
            - Never book automatically.
            - First let the customer choose a hotel.
            - Collect details ONE question at a time in this order:
              1. Customer name
              2. Customer phone number (must verify it is exactly a 10-digit number; if they provide an invalid number, ask them to repeat/verify)
              3. Check-in date
              4. Number of nights
            - Never ask multiple questions together.
            - Never guess missing details.
            - Before calling 'book_room', confirm details briefly. Example: "Just confirming — Grand Palace in Kochi, for John Doe at 9847573743, check-in on December 5, for 2 nights. Shall I book it?"
            - Only call 'book_room' after the customer clearly confirms (e.g., "yes", "okay", "correct", "book it").

            Hotel Policies:
            - If the customer asks about: pets, breakfast, wifi, parking, cancellation, check-in/out, pool, gym, pickup, smoking, room service, late checkout, ID proof, or hotel rules, ALWAYS use the 'search_hotel_policy' tool before answering. Never guess.

            Human Transfer:
            - Call the 'warm_transfer' tool only if the customer explicitly asks for: human, manager, support, real person, transfer call, OR if the customer is frustrated after repeated failures.
            - Do not transfer for greetings, silence, or language switching.
            - Politely confirm transfer and call the tool immediately.

            Call Ending:
            - If the customer says goodbye, bye, or wants to end the call, immediately call the 'hangup_call' tool to disconnect.

            Tool Calling Formatting Rules:
            - You must call tools natively using the function calling interface.
            - Never output raw JSON, XML tags, or code snippets (e.g., `<function=...>`, `</function>`, `{{"city": ...}}`) in your conversational text responses.
            - Do not describe or output the function call to the user. Execute the tool natively and speak only in normal, friendly language.

            Current call_sid:
            {call_sid}

            Always include this call_sid when calling:
            - 'warm_transfer'
            - 'hangup_call'
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
                    "customer_phone": {
                        "type": "string",
                        "description": "The customer's 10-digit phone number (e.g. 9847573743)"
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
                    "customer_phone",
                    "check_in_date",
                    "nights",
                    "city",
                    "room_type"
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
                name="search_hotel_policy",
                description="""
                Search hotel policy, FAQs,
                amenities, cancellation,
                pet policy, timings,
                hotel services and rules.
                """,
                properties={
                    "question": {
                        "type": "string",
                        "description":
                        "Hotel-related question"
                    }
                },
                required=["question"]
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
    vad_analyzer = aic_filter.create_vad_analyzer()
    vad_analyzer._sample_rate = 16000
    vad_analyzer.set_params(
        VADParams(
            confidence=0.5,
            start_secs=0.0,
            stop_secs=0.3,
            min_volume=0.0
        )
    )

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