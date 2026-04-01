"""
Claude API wrapper for Client Follow-Up Autopilot.
Handles email generation, context extraction, and response classification.
Uses style examples from the learning engine for few-shot prompting.
"""

import json
import logging

import anthropic

from config import ANTHROPIC_API_KEY, CLAUDE_MODEL, SUPPORTED_LANGUAGES

logger = logging.getLogger(__name__)

# ─── System Prompts by Stage and Language ────────────────────────────────────

TONE_MAP = {
    1: {
        "ES": "amable y servicial. Este es un recordatorio gentil",
        "EN": "friendly and helpful. This is a gentle reminder",
        "PT": "amigável e prestativo. Este é um lembrete gentil",
    },
    2: {
        "ES": "profesional y un poco más directo. Este es un segundo aviso",
        "EN": "professional and slightly more direct. This is a second notice",
        "PT": "profissional e um pouco mais direto. Este é um segundo aviso",
    },
    3: {
        "ES": "urgente pero respetuoso, enfatizando el impacto en el timeline",
        "EN": "urgent but respectful, emphasizing the impact on the timeline",
        "PT": "urgente mas respeitoso, enfatizando o impacto no cronograma",
    },
    4: {
        "ES": "formal y de escalamiento, dirigido a un contacto senior, mencionando que intentos previos al contacto principal no han tenido respuesta",
        "EN": "formal and escalatory, addressed to a senior contact, noting that previous attempts to the primary contact have not received a response",
        "PT": "formal e de escalação, dirigido a um contato sênior, mencionando que tentativas anteriores ao contato principal não receberam resposta",
    },
}

LANGUAGE_INSTRUCTIONS = {
    "ES": "Escribe completamente en español.",
    "EN": "Write entirely in English.",
    "PT": "Escreva completamente em português.",
}


def _get_client():
    """Return an Anthropic client instance."""
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY not set in .env")
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def _call_claude_with_retry(system_prompt, user_prompt, max_tokens=1024, retries=3):
    """
    Call Claude API with retry logic and exponential backoff.

    Args:
        system_prompt: System prompt string
        user_prompt: User prompt string
        max_tokens: Max tokens for response
        retries: Number of retry attempts

    Returns:
        Response text string, or None on failure
    """
    import time

    client = _get_client()
    for attempt in range(retries):
        try:
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            return response.content[0].text.strip()
        except anthropic.RateLimitError as e:
            wait_time = 2 ** attempt * 5  # 5s, 10s, 20s
            logger.warning(f"Claude rate limited. Retrying in {wait_time}s (attempt {attempt + 1}/{retries})")
            time.sleep(wait_time)
        except anthropic.APIError as e:
            if attempt < retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"Claude API error: {e}. Retrying in {wait_time}s (attempt {attempt + 1}/{retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Claude API error after {retries} attempts: {e}")
                return None
        except Exception as e:
            logger.error(f"Claude unexpected error: {e}")
            return None
    return None


def _parse_json_response(response_text):
    """Parse JSON from Claude response, handling markdown wrapping."""
    if not response_text:
        return None
    text = response_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1])
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"Claude response is not valid JSON: {e}\nResponse: {text[:500]}")
        return None


def _build_system_prompt(company_name, language, stage, style_examples=None, sender_name=None):
    """Build the system prompt for follow-up email generation."""
    tone = TONE_MAP.get(stage, TONE_MAP[1]).get(language, TONE_MAP[1]["EN"])
    lang_instruction = LANGUAGE_INSTRUCTIONS.get(language, LANGUAGE_INSTRUCTIONS["EN"])

    # Sender name for signature
    signer = sender_name or "El equipo"

    prompt = f"""You are a professional Client Success assistant for {company_name}.
You write follow-up emails to clients requesting specific information or documents
that {company_name} needs FROM the client in order to complete a project deliverable.
Your tone is {tone}. {lang_instruction}

You ALWAYS include:
- The specific project name
- The exact information or documents the client needs to provide (from "Information Needed")
- Why this information is important for the project progress
- A polite question asking WHEN they can send the information (e.g. "¿Podrían indicarnos en qué fecha nos podrían hacer llegar esta información?")
- A clear, specific call to action

You NEVER:
- Use bold text (<strong>, <b>) in the email body — keep it plain and natural
- Include specific deadlines or due dates for when the client should send the information
- Mention internal deliverable names or milestone numbers (like "Constancia del envío a auditoría")
- Include threats or blame
- Reference internal processes the client doesn't need to know about
- Sign as "Equipo de Leaf", "Equipo Leaf", "El equipo de {company_name}" or any team-based signature
- Include links to documents — if documents need to be shared, they will be attached to the email

The email signature must use the sender's personal name: "{signer}".
Example signature: "Saludos cordiales,\\n{signer}"

Keep the email under 150 words. Be helpful, warm, and professional.

IMPORTANT: Return ONLY valid JSON with keys "subject" and "body_html".
The body_html should be clean HTML suitable for email (use <p>, <br> tags only, NO <strong> or <b>).
Do not include any text outside the JSON object."""

    if style_examples:
        prompt += "\n\nHere are examples of the communication style preferred by the team. Match this style:\n"
        for i, example in enumerate(style_examples[:3], 1):
            prompt += f"\n--- Example {i} ---\n{example}\n"

    return prompt


# ─── Email Generation ───────────────────────────────────────────────────────

def generate_followup_email(context, language, stage, company_name="", style_examples=None, sender_name=None):
    """
    Generate a follow-up email using Claude API.

    Args:
        context: Dict with keys:
            - project_name: str
            - client_name: str
            - pending_item: str
            - due_date: str
            - days_overdue: int
            - impact_description: str
            - follow_up_stage: int (previous stage)
        language: 'ES', 'EN', or 'PT'
        stage: Current stage number (1-4)
        company_name: Company name for system prompt
        style_examples: Optional list of style example strings from learning engine
        sender_name: Name of the CS person sending the email (for signature)

    Returns:
        Dict with 'subject' and 'body_html', or None on failure
    """
    if language not in SUPPORTED_LANGUAGES:
        logger.warning(f"Unsupported language '{language}', defaulting to EN")
        language = "EN"

    system_prompt = _build_system_prompt(company_name, language, stage, style_examples, sender_name=sender_name)

    user_prompt = f"""Generate a follow-up email with these details:
- Project: {context.get('project_name', 'N/A')}
- Client: {context.get('client_name', 'N/A')}
- Information Needed from Client: {context.get('information_needed', context.get('impact_description', 'N/A'))}
- Deliverable Context (internal, do NOT mention to client): {context.get('pending_item', 'N/A')}
- Previous follow-ups sent: {stage - 1}
- Language: {language}

The email should:
1. Ask the client to provide the specific information/documents listed in "Information Needed"
2. Politely ask WHEN they could send the information (do NOT set a deadline)
3. Do NOT reference the internal deliverable name
4. Do NOT use bold text
5. Do NOT include links to documents (they will be attached separately)

Return JSON with keys: "subject", "body_html"
"""

    try:
        response_text = _call_claude_with_retry(system_prompt, user_prompt, max_tokens=1024)
        result = _parse_json_response(response_text)

        if not result:
            return None

        if "subject" not in result or "body_html" not in result:
            logger.error(f"Claude response missing required keys: {result.keys()}")
            return None

        logger.info(f"Generated follow-up email for {context.get('project_name')} (Stage {stage}, {language})")
        return result

    except Exception as e:
        logger.error(f"Claude unexpected error: {e}")
        return None


# ─── Context Extraction ─────────────────────────────────────────────────────

def extract_context(raw_text, company_name=""):
    """
    Extract structured context from a raw team message (email or Slack).
    Identifies which project, client, and information is being conveyed.

    Args:
        raw_text: The raw message text from a team member
        company_name: Company name for context

    Returns:
        Dict with 'project_name', 'client_name', 'information_type',
        'summary', 'action_needed', or None on failure
    """
    system_prompt = f"""You are an assistant for {company_name}'s Client Success team.
Your job is to extract structured information from internal team messages.
These messages contain updates, reviews, checklists, or deliverables that need to be relayed to clients.

Return ONLY valid JSON with these keys:
- "project_name": The project this relates to (string)
- "client_name": The client this is for (string, or null if not clear)
- "information_type": What kind of info (e.g., "review", "checklist", "deliverable", "update")
- "summary": A brief summary of the key information (1-2 sentences)
- "action_needed": What the client needs to do with this information (string)
- "confidence": How confident you are this extraction is correct (0.0-1.0)

If you cannot identify the project or the message is unclear, set confidence below 0.5."""

    try:
        response_text = _call_claude_with_retry(
            system_prompt,
            f"Extract context from this team message:\n\n{raw_text}",
            max_tokens=512,
        )
        result = _parse_json_response(response_text)
        if result:
            logger.info(f"Extracted context: project={result.get('project_name')}, confidence={result.get('confidence')}")
        return result

    except Exception as e:
        logger.error(f"Context extraction error: {e}")
        return None


# ─── Response Classification ────────────────────────────────────────────────

def classify_response(email_body, pending_item=""):
    """
    Classify a client's email response to determine if they actually
    provided the requested information.

    Args:
        email_body: The client's email text
        pending_item: What was being requested

    Returns:
        Dict with 'classification' (one of: 'received', 'partial', 'question', 'unrelated'),
        'confidence' (0.0-1.0), and 'summary' (brief explanation)
    """
    system_prompt = """You classify client email responses to follow-up requests.
Determine if the client has provided the requested information.

Return ONLY valid JSON with keys:
- "classification": One of "received" (they sent what was asked), "partial" (some but not all), "question" (they're asking a question), "unrelated" (not about this request)
- "confidence": How confident you are (0.0-1.0)
- "summary": Brief explanation of what the client said/sent (1 sentence)"""

    try:
        response_text = _call_claude_with_retry(
            system_prompt,
            f"We requested: {pending_item}\n\nClient responded:\n{email_body}",
            max_tokens=256,
        )
        result = _parse_json_response(response_text)
        if result:
            logger.info(f"Classified response: {result.get('classification')} (confidence: {result.get('confidence')})")
        return result

    except Exception as e:
        logger.error(f"Response classification error: {e}")
        return None


if __name__ == "__main__":
    # Quick connectivity test
    logging.basicConfig(level=logging.INFO)
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set in .env")
    else:
        try:
            client = _get_client()
            # Simple test call
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=50,
                messages=[{"role": "user", "content": "Say 'connection successful' in 3 words or less."}],
            )
            print(f"SUCCESS: Connected to Claude API. Response: {response.content[0].text}")
        except Exception as e:
            print(f"ERROR: Could not connect to Claude API: {e}")
