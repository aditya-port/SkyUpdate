import os
from dotenv import load_dotenv
from google import genai

# Load env
load_dotenv()

# Init client
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Request
response = client.models.generate_content(
    model="gemini-2.0-flash",
    contents="hi"
)

print(response.text)