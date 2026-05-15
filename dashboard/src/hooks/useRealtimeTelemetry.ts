import { useEffect, useMemo, useState } from 'react';
import type { DlqMessage, OutboxMessage, PlaneEvent, QueueInfo, TelemetrySnapshot } from '../types/telemetry';

const initialQueues: QueueInfo[] = [
  {
    name: 'orders.created',
    topic: 'plane.orders.created',
    depth: 184,
    throughput: 76,
    latencyMs: 18,
    errorRate: 0.4,
    subscribers: [
      { id: 'sub-payments-01', service: 'payments-api', consumer: 'durable-payments', lag: 8, inflight: 14, status: 'online' },
      { id: 'sub-fraud-01', service: 'fraud-checker', consumer: 'durable-fraud', lag: 21, inflight: 9, status: 'online' },
    ],
  },
  {
    name: 'billing.settled',
    topic: 'plane.billing.settled',
    depth: 41,
    throughput: 33,
    latencyMs: 27,
    errorRate: 1.1,
    subscribers: [
      { id: 'sub-ledger-01', service: 'ledger-writer', consumer: 'durable-ledger', lag: 3, inflight: 5, status: 'online' },
      { id: 'sub-notify-01', service: 'notification-hub', consumer: 'durable-notify', lag: 11, inflight: 3, status: 'degraded' },
    ],
  },
  {
    name: 'inventory.adjusted',
    topic: 'plane.inventory.adjusted',
    depth: 92,
    throughput: 48,
    latencyMs: 22,
    errorRate: 0.7,
    subscribers: [
      { id: 'sub-catalog-01', service: 'catalog-sync', consumer: 'durable-catalog', lag: 17, inflight: 7, status: 'online' },
      { id: 'sub-erp-01', service: 'erp-bridge', consumer: 'durable-erp', lag: 45, inflight: 2, status: 'degraded' },
    ],
  },
  {
    name: 'shipping.failed',
    topic: 'plane.shipping.failed',
    depth: 12,
    throughput: 9,
    latencyMs: 64,
    errorRate: 3.8,
    subscribers: [{ id: 'sub-support-01', service: 'support-router', consumer: 'durable-support', lag: 2, inflight: 1, status: 'online' }],
  },
];

const initialDlq: DlqMessage[] = [
  { id: 'dlq-8842', queue: 'shipping.failed', reason: 'subscriber timeout after 30s', attempts: 5, payload: '{"shipmentId":"SHP-8812","carrier":"azul"}', lastSeen: new Date(Date.now() - 45_000).toISOString() },
  { id: 'dlq-8841', queue: 'billing.settled', reason: 'schema validation failed: amount', attempts: 3, payload: '{"invoiceId":"INV-2209","amount":null}', lastSeen: new Date(Date.now() - 180_000).toISOString() },
];

const initialOutbox: OutboxMessage[] = [
  { id: 'out-4412', aggregate: 'Order#9812', destination: 'orders.created', state: 'pending', createdAt: new Date(Date.now() - 22_000).toISOString(), nextAttempt: 'em 3s' },
  { id: 'out-4411', aggregate: 'Invoice#7721', destination: 'billing.settled', state: 'published', createdAt: new Date(Date.now() - 58_000).toISOString(), nextAttempt: 'publicado' },
  { id: 'out-4410', aggregate: 'Stock#A-118', destination: 'inventory.adjusted', state: 'failed', createdAt: new Date(Date.now() - 320_000).toISOString(), nextAttempt: 'em 2min' },
];

function randomFrom<T>(items: T[]) {
  return items[Math.floor(Math.random() * items.length)];
}

function createEvent(queues: QueueInfo[]): PlaneEvent {
  const queue = randomFrom(queues);
  const status = randomFrom(['ack', 'pending', 'retrying', 'failed'] as const);
  return {
    id: `evt-${crypto.randomUUID().slice(0, 8)}`,
    queue: queue.name,
    subject: queue.topic,
    producer: randomFrom(['checkout-api', 'billing-worker', 'stock-service', 'shipment-orchestrator']),
    payload: JSON.stringify({ traceId: crypto.randomUUID(), tenant: randomFrom(['ubiq', 'allas', 'plane']), version: 1 }),
    status,
    timestamp: new Date().toISOString(),
    latencyMs: queue.latencyMs + Math.random() * 24,
  };
}

function normalizeSnapshot(data: Partial<TelemetrySnapshot>): Partial<TelemetrySnapshot> {
  return {
    queues: data.queues,
    events: data.events,
    dlq: data.dlq,
    outbox: data.outbox,
  };
}

export function useRealtimeTelemetry(): TelemetrySnapshot {
  const [queues, setQueues] = useState(initialQueues);
  const [events, setEvents] = useState<PlaneEvent[]>(() => Array.from({ length: 8 }, () => createEvent(initialQueues)));
  const [dlq, setDlq] = useState(initialDlq);
  const [outbox, setOutbox] = useState(initialOutbox);
  const [connected, setConnected] = useState(false);
  const wsUrl = import.meta.env.VITE_TELEMETRY_WS_URL as string | undefined;

  useEffect(() => {
    if (!wsUrl) return;

    const ws = new WebSocket(wsUrl);
    ws.addEventListener('open', () => setConnected(true));
    ws.addEventListener('close', () => setConnected(false));
    ws.addEventListener('message', (message) => {
      const data = normalizeSnapshot(JSON.parse(message.data) as Partial<TelemetrySnapshot>);
      if (data.queues) setQueues(data.queues);
      if (data.events) setEvents((current) => [...data.events!, ...current].slice(0, 80));
      if (data.dlq) setDlq(data.dlq);
      if (data.outbox) setOutbox(data.outbox);
    });

    return () => ws.close();
  }, [wsUrl]);

  useEffect(() => {
    if (wsUrl && connected) return;

    const interval = window.setInterval(() => {
      setEvents((current) => [createEvent(queues), ...current].slice(0, 80));
      setQueues((current) =>
        current.map((queue) => ({
          ...queue,
          depth: Math.max(0, queue.depth + Math.floor(Math.random() * 15) - 6),
          throughput: Math.max(1, queue.throughput + Math.floor(Math.random() * 9) - 4),
          latencyMs: Math.max(5, queue.latencyMs + Math.floor(Math.random() * 9) - 4),
          subscribers: queue.subscribers.map((subscriber) => ({
            ...subscriber,
            lag: Math.max(0, subscriber.lag + Math.floor(Math.random() * 7) - 3),
            inflight: Math.max(0, subscriber.inflight + Math.floor(Math.random() * 5) - 2),
          })),
        })),
      );
    }, 1_400);

    return () => window.clearInterval(interval);
  }, [connected, queues, wsUrl]);

  return useMemo(
    () => ({ connected: connected || !wsUrl, source: wsUrl ? 'websocket' : 'simulator', queues, events, dlq, outbox }),
    [connected, dlq, events, outbox, queues, wsUrl],
  );
}
