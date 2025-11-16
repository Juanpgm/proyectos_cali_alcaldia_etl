"""
Script de prueba para la función de normalización
"""
import sys
import re
from difflib import SequenceMatcher

def title_case_with_exceptions(text: str) -> str:
    """Versión de prueba de title_case_with_exceptions"""
    lowercase_words = {
        'de', 'del', 'la', 'el', 'los', 'las', 'y', 'e', 'o', 'u',
        'a', 'ante', 'bajo', 'con', 'contra', 'desde', 'en', 'entre',
        'hacia', 'hasta', 'para', 'por', 'según', 'sin', 'sobre',
        'tras', 'al', 'un', 'una', 'unos', 'unas'
    }
    
    words = text.split()
    formatted_words = []
    
    for i, word in enumerate(words):
        word = word.strip()
        if not word:
            continue
        
        # Preserve placeholders
        if '___RSRVD' in word.upper() and word.endswith('___'):
            formatted_words.append(word)
            continue
            
        # First word always capitalized
        if i == 0:
            formatted_words.append(word.capitalize())
        # Check if word is a connector/article
        elif word.lower() in lowercase_words:
            formatted_words.append(word.lower())
        # Regular word
        else:
            formatted_words.append(word.capitalize())
    
    return ' '.join(formatted_words)


def normalize_nombre_up_test(text: str) -> str:
    """Versión de prueba de normalize_nombre_up"""
    if not text:
        return text
    
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    
    # Define reserved words
    reserved_patterns = [
        r'\bI\.E\.?\b',
        r'\bIPS\b',
        r'\bUTS\b',
        r'\bCALI\b',
    ]
    
    # Store reserved words
    reserved_words = []
    for idx, pattern in enumerate(reserved_patterns):
        placeholder = f"___RSRVD{idx:02d}___"
        
        matches = list(re.finditer(pattern, text, flags=re.IGNORECASE))
        for match in reversed(matches):
            original = match.group(0)
            reserved_words.append((placeholder, original))
            text = text[:match.start()] + placeholder + text[match.end():]
    
    # Clean spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Apply title case
    text = title_case_with_exceptions(text)
    
    # Restore reserved words
    for placeholder, original in reversed(reserved_words):
        text = text.replace(placeholder, original)
    
    return text.strip()


# Test cases
test_cases = [
    "IPS - Union de Vivienda Popular",
    "I.E. Santa Cecilia",
    "I.E Santa Librada",
    "UTS las Garzas",
    "Normal Superior de CALI",
    "Autopista CALI - Palmira",
]

print("PRUEBAS DE NORMALIZACIÓN")
print("=" * 60)

for test in test_cases:
    result = normalize_nombre_up_test(test)
    print(f"\nOriginal: {test}")
    print(f"Resultado: {result}")
