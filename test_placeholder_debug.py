"""
Prueba de debug del sistema de placeholders
"""
import re

text = "I.E. Santa Cecilia"
print(f"Texto original: {text}")

# Patrón
pattern = r'\bI\.E\.?\b'
placeholder = "___RSRVD00___"

# Encontrar coincidencias
matches = list(re.finditer(pattern, text, flags=re.IGNORECASE))
print(f"\nCoincidencias encontradas: {len(matches)}")

reserved_words = []
for match in reversed(matches):
    original = match.group(0)
    print(f"  - Original: '{original}' en posición {match.start()}-{match.end()}")
    reserved_words.append((placeholder, original))
    text = text[:match.start()] + placeholder + text[match.end():]

print(f"\nTexto con placeholders: {text}")
print(f"Palabras reservadas: {reserved_words}")

# Simular title case
text_lower = text.lower()
print(f"\nTexto en minúsculas: {text_lower}")

# Intentar restaurar
for ph, orig in reversed(reserved_words):
    print(f"\nBuscando placeholder '{ph}' para reemplazar con '{orig}'")
    print(f"  ¿Está '{ph}' en '{text}'? {ph in text}")
    print(f"  ¿Está '{ph.lower()}' en '{text_lower}'? {ph.lower() in text_lower}")
    text = text.replace(ph, orig)
    text_lower = text_lower.replace(ph.lower(), orig)

print(f"\nTexto final (mayúsculas): {text}")
print(f"Texto final (minúsculas): {text_lower}")
