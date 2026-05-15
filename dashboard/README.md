# UbiQ Plane Events Dashboard

Dashboard Vite + React + Tailwind + animate.css com componentes no padrão shadcn/ui para gerenciamento e telemetria em tempo real.

## Rodando em desenvolvimento

```bash
cd dashboard
npm install
npm run dev
```

O servidor Vite está configurado com `host: true` e HMR/hot reload habilitado. O script `dev` também usa `--host 0.0.0.0` para expor a UI dentro de containers.

## Fonte de telemetria

Por padrão, a interface usa um simulador local para demonstrar o fluxo ao vivo. Para conectar uma fonte real, defina:

```bash
VITE_TELEMETRY_WS_URL=ws://localhost:8080/events npm run dev
```

O WebSocket pode enviar snapshots parciais em JSON com as chaves `queues`, `events`, `dlq` e `outbox`.

## Telas incluídas

- Eventos emitidos em tempo real com cor de fundo diferente por fila.
- Lista de filas com seus subscribers, lag, inflight e status.
- DLQ com motivo da falha, tentativas e ações de reprocessamento/arquivamento.
- Outbox com status transacional de publicação.
