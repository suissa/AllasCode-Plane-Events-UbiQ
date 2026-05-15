export type EventStatus = 'ack' | 'pending' | 'retrying' | 'failed' | 'dlq' | 'outbox';

export interface QueueSubscriber {
  id: string;
  service: string;
  consumer: string;
  lag: number;
  inflight: number;
  status: 'online' | 'degraded' | 'offline';
}

export interface QueueInfo {
  name: string;
  topic: string;
  depth: number;
  throughput: number;
  latencyMs: number;
  errorRate: number;
  subscribers: QueueSubscriber[];
}

export interface PlaneEvent {
  id: string;
  queue: string;
  subject: string;
  producer: string;
  payload: string;
  status: EventStatus;
  timestamp: string;
  latencyMs: number;
}

export interface DlqMessage {
  id: string;
  queue: string;
  reason: string;
  attempts: number;
  payload: string;
  lastSeen: string;
}

export interface OutboxMessage {
  id: string;
  aggregate: string;
  destination: string;
  state: 'pending' | 'published' | 'failed';
  createdAt: string;
  nextAttempt: string;
}

export interface TelemetrySnapshot {
  connected: boolean;
  source: 'websocket' | 'simulator';
  queues: QueueInfo[];
  events: PlaneEvent[];
  dlq: DlqMessage[];
  outbox: OutboxMessage[];
}
