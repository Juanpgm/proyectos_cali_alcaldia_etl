import requests

response = requests.get('https://gestorproyectoapi-production.up.railway.app/contratos_emprestito_all', timeout=30)
data = response.json()['data']

print(f"Total contratos: {len(data)}")
print(f"\nPrimer contrato:")
primer = data[0]
print(f"  valor_pagado existe: {'valor_pagado' in primer}")
print(f"  valor_pagado: {primer.get('valor_pagado', 'N/A')}")
print(f"  valor_contrato: {primer.get('valor_contrato', 'N/A')}")

# Verificar cuántos tienen valor_pagado válido (no vacío, no "0")
con_valor_pagado_valido = []
for c in data:
    vp = c.get('valor_pagado', '')
    try:
        vp_float = float(vp) if vp else 0
        if vp_float > 0:
            con_valor_pagado_valido.append(c)
    except:
        pass

print(f"\n📊 Resumen:")
print(f"  Total contratos: {len(data)}")
print(f"  Con valor_pagado > 0: {len(con_valor_pagado_valido)}")
print(f"  Con valor_pagado = 0 o vacío: {len(data) - len(con_valor_pagado_valido)}")

# Mostrar algunos ejemplos
if con_valor_pagado_valido:
    print(f"\n📋 Ejemplos con valor_pagado > 0:")
    for i, c in enumerate(con_valor_pagado_valido[:5]):
        valor_pag = c.get('valor_pagado')
        try:
            vp_num = float(valor_pag)
            print(f"{i+1}. {c.get('referencia_contrato')}: valor_pagado=${vp_num:,.0f}")
        except:
            print(f"{i+1}. {c.get('referencia_contrato')}: valor_pagado={valor_pag}")
else:
    print(f"\n⚠️  No hay contratos con valor_pagado > 0")
