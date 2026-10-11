# AGENTS.md — Reglas de la sesión

## Fuente de verdad
- **docs/spec.md** es la única fuente de verdad. Si algo es ambiguo o contradictorio, **preguntá antes de decidir**.

## Modo de trabajo
- Trabajá **SOLO el paso que te pida**. No adelantes funcionalidad de pasos futuros.
- Al terminar cada paso, listá:
  1. Archivos creados/modificados
  2. Comandos exactos para verificarlo a mano
  3. Qué resultado espero ver
- Nada de tests automatizados salvo que los pida. La verificación es **manual**.

## Estándares técnicos
- **TypeScript estricto** (`strict: true` en tsconfig)
- **pnpm workspaces** (monorepo)
- **Node LTS** (versión exacta fijada en `.nvmrc` y `package.json`)
- **Versiones exactas** de dependencias e imágenes Docker (sin `latest`, sin rangos)

## Git
- No commitees salvo que se pida explícitamente.