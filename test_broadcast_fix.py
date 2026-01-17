import asyncio
import sys
from pathlib import Path

def test_extraction_logic():
    print("Testing raw message extraction logic...")
    
    # Simulate /broadcast <target> <message_body>
    test_cases = [
        ("/broadcast subs Hello\nWorld", "Hello\nWorld"),
        ("/broadcast all   Multiple Spaces", "Multiple Spaces"),
        ("/broadcast free <b>Bold</b> and <code>Code</code>", "<b>Bold</b> and <code>Code</code>"),
        ("/broadcast subs <tg-spoiler>Secret</tg-spoiler>", "<tg-spoiler>Secret</tg-spoiler>"),
        ("/broadcast all Line1<br>Line2", "Line1\nLine2"), # Backwards compatibility
    ]
    
    for full_text, expected_msg in test_cases:
        # This mirrors the logic in admin_commands.py:
        parts = full_text.split(None, 2)
        msg_text = parts[2] if len(parts) >= 3 else ""
        
        # Mirror the <br> replacement logic:
        if msg_text:
            msg_text = msg_text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
            
        print(f"Full: {repr(full_text)} -> Extracted: {repr(msg_text)}")
        assert msg_text == expected_msg
    
    print("✅ Success: All extraction and formatting cases passed!")

if __name__ == "__main__":
    test_extraction_logic()
