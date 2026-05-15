import { Activity, AlertTriangle, Boxes, Clock3, DatabaseZap, Radio, RefreshCw, Send, Users } from 'lucide-react';
import { useMemo, useState } from 'react';
import { Badge } from './components/ui/badge';
import { Button } from './components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './components/ui/tabs';
import { useRealtimeTelemetry } from './hooks/useRealtimeTelemetry';
import { cn, formatLatency, queueGradient, relativeTime } from './lib/utils';
import type { EventStatus } from './types/telemetry';

const statusVariant: Record<EventStatus, 'default' | 'success' | 'warning' | 'destructive' | 'secondary'> = {
  ack: 'success',
  pending: 'warning',
  retrying: 'default',
  failed: 'destructive',
  dlq: 'destructive',
  outbox: 'secondary',
};

export function App() {
  const telemetry = useRealtimeTelemetry();
  const [tab, setTab] = useState('events');
  const totals = useMemo(
    () => ({
      depth: telemetry.queues.reduce((total, queue) => total + queue.depth, 0),
      throughput: telemetry.queues.reduce((total, queue) => total + queue.throughput, 0),
      subscribers: telemetry.queues.reduce((total, queue) => total + queue.subscribers.length, 0),
      avgLatency: telemetry.queues.reduce((total, queue) => total + queue.latencyMs, 0) / telemetry.queues.length,
    }),
    [telemetry.queues],
  );

  return (
    <main className="min-h-screen overflow-hidden bg-slate-950 text-slate-100">
      <div className="absolute inset-0 -z-0 bg-[radial-gradient(circle_at_top_left,rgba(14,165,233,0.22),transparent_35%),radial-gradient(circle_at_bottom_right,rgba(168,85,247,0.18),transparent_35%)]" />
      <section className="relative z-10 mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 py-6 lg:px-8">
        <header className="flex flex-col gap-4 rounded-3xl border border-white/10 bg-white/[0.04] p-6 shadow-glow backdrop-blur md:flex-row md:items-center md:justify-between">
          <div className="animate__animated animate__fadeInLeft">
            <div className="mb-3 flex items-center gap-2 text-sm font-medium text-cyan-200">
              <Radio className="h-4 w-4 animate-pulse" /> Telemetria em tempo real · {telemetry.source === 'websocket' ? 'WebSocket' : 'simulador local'}
            </div>
            <h1 className="text-3xl font-bold tracking-tight text-white md:text-5xl">UbiQ Plane Events</h1>
            <p className="mt-3 max-w-2xl text-slate-300">Dashboard operacional para filas, subscribers, eventos emitidos, DLQ e Outbox com hot reload via Vite.</p>
          </div>
          <Badge variant={telemetry.connected ? 'success' : 'destructive'} className="w-fit gap-2 px-4 py-2 text-sm">
            <span className={cn('h-2 w-2 rounded-full', telemetry.connected ? 'bg-emerald-300' : 'bg-rose-300')} />
            {telemetry.connected ? 'stream conectado' : 'stream desconectado'}
          </Badge>
        </header>

        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <MetricCard icon={Boxes} label="Profundidade total" value={totals.depth.toLocaleString('pt-BR')} helper="mensagens aguardando" />
          <MetricCard icon={Activity} label="Throughput" value={`${totals.throughput}/s`} helper="eventos emitidos" />
          <MetricCard icon={Users} label="Subscribers" value={totals.subscribers.toString()} helper="consumidores ativos" />
          <MetricCard icon={Clock3} label="Latência média" value={formatLatency(totals.avgLatency)} helper="p95 operacional" />
        </div>

        <Tabs value={tab} onValueChange={setTab}>
          <TabsList className="flex h-auto w-full flex-wrap justify-start gap-2 bg-slate-900/70 p-2 md:w-fit">
            <TabsTrigger value="events">Eventos em tempo real</TabsTrigger>
            <TabsTrigger value="queues">Filas & Subscribers</TabsTrigger>
            <TabsTrigger value="dlq">DLQ</TabsTrigger>
            <TabsTrigger value="outbox">Outbox</TabsTrigger>
          </TabsList>

          <TabsContent value="events">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2"><DatabaseZap className="h-5 w-5 text-cyan-300" /> Eventos emitidos</CardTitle>
                <CardDescription>Cada fila usa uma cor de fundo diferente para facilitar a leitura do fluxo em tempo real.</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid max-h-[620px] gap-3 overflow-y-auto pr-2">
                  {telemetry.events.map((event) => (
                    <article key={`${event.id}-${event.timestamp}`} className={cn('animate__animated animate__fadeInUp rounded-2xl border bg-gradient-to-r p-4', queueGradient(event.queue))}>
                      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                        <div>
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="font-mono text-sm text-white/80">{event.id}</span>
                            <Badge variant={statusVariant[event.status]}>{event.status}</Badge>
                            <Badge variant="secondary">{event.queue}</Badge>
                          </div>
                          <h3 className="mt-2 text-lg font-semibold text-white">{event.subject}</h3>
                          <p className="mt-1 text-sm text-slate-300">producer: {event.producer} · latência {formatLatency(event.latencyMs)} · {relativeTime(event.timestamp)}</p>
                        </div>
                        <code className="max-w-xl overflow-x-auto rounded-lg border border-white/10 bg-slate-950/70 px-3 py-2 text-xs text-cyan-100">{event.payload}</code>
                      </div>
                    </article>
                  ))}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="queues">
            <div className="grid gap-4 xl:grid-cols-2">
              {telemetry.queues.map((queue) => (
                <Card key={queue.name} className={cn('border bg-gradient-to-br', queueGradient(queue.name))}>
                  <CardHeader>
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <CardTitle>{queue.name}</CardTitle>
                        <CardDescription>{queue.topic}</CardDescription>
                      </div>
                      <Badge variant={queue.errorRate > 2 ? 'destructive' : 'success'}>{queue.errorRate.toFixed(1)}% erro</Badge>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="mb-4 grid grid-cols-3 gap-3 text-sm">
                      <QueueStat label="Depth" value={queue.depth} />
                      <QueueStat label="Throughput" value={`${queue.throughput}/s`} />
                      <QueueStat label="Latency" value={formatLatency(queue.latencyMs)} />
                    </div>
                    <div className="space-y-2">
                      {queue.subscribers.map((subscriber) => (
                        <div key={subscriber.id} className="rounded-xl border border-white/10 bg-slate-950/55 p-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div>
                              <p className="font-medium text-white">{subscriber.service}</p>
                              <p className="text-xs text-slate-400">{subscriber.consumer} · {subscriber.id}</p>
                            </div>
                            <Badge variant={subscriber.status === 'online' ? 'success' : subscriber.status === 'degraded' ? 'warning' : 'destructive'}>{subscriber.status}</Badge>
                          </div>
                          <div className="mt-3 grid grid-cols-2 gap-2 text-xs text-slate-300">
                            <span>Lag: <b className="text-white">{subscriber.lag}</b></span>
                            <span>Inflight: <b className="text-white">{subscriber.inflight}</b></span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </TabsContent>

          <TabsContent value="dlq">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2"><AlertTriangle className="h-5 w-5 text-amber-300" /> Dead Letter Queue</CardTitle>
                <CardDescription>Mensagens que excederam tentativas, falharam validação ou perderam SLA de processamento.</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                {telemetry.dlq.map((message) => (
                  <div key={message.id} className={cn('rounded-2xl border bg-gradient-to-r p-4', queueGradient(message.queue))}>
                    <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                      <div>
                        <div className="flex flex-wrap items-center gap-2"><Badge variant="destructive">{message.id}</Badge><Badge variant="secondary">{message.queue}</Badge><span className="text-sm text-slate-300">{relativeTime(message.lastSeen)}</span></div>
                        <p className="mt-2 font-medium text-white">{message.reason}</p>
                        <p className="mt-1 text-sm text-slate-300">tentativas: {message.attempts} · payload {message.payload}</p>
                      </div>
                      <div className="flex gap-2"><Button size="sm"><RefreshCw className="mr-2 h-4 w-4" />Reprocessar</Button><Button size="sm" variant="outline">Arquivar</Button></div>
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="outbox">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2"><Send className="h-5 w-5 text-cyan-300" /> Outbox</CardTitle>
                <CardDescription>Controle transacional dos eventos aguardando publicação ou confirmação de envio.</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="overflow-hidden rounded-2xl border border-white/10">
                  <div className="grid grid-cols-5 bg-white/10 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-400">
                    <span>ID</span><span>Aggregate</span><span>Destino</span><span>Status</span><span>Próxima tentativa</span>
                  </div>
                  {telemetry.outbox.map((item) => (
                    <div key={item.id} className={cn('grid grid-cols-5 items-center border-t border-white/10 px-4 py-3 text-sm', item.state === 'failed' ? 'bg-rose-500/10' : 'bg-slate-950/45')}>
                      <span className="font-mono text-cyan-100">{item.id}</span>
                      <span>{item.aggregate}</span>
                      <span>{item.destination}</span>
                      <span><Badge variant={item.state === 'published' ? 'success' : item.state === 'pending' ? 'warning' : 'destructive'}>{item.state}</Badge></span>
                      <span className="text-slate-300">{item.nextAttempt}</span>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </section>
    </main>
  );
}

function MetricCard({ icon: Icon, label, value, helper }: { icon: typeof Activity; label: string; value: string; helper: string }) {
  return (
    <Card className="animate__animated animate__fadeInUp">
      <CardContent className="flex items-center gap-4 p-5">
        <div className="rounded-2xl bg-cyan-400/15 p-3 text-cyan-200"><Icon className="h-6 w-6" /></div>
        <div><p className="text-sm text-slate-400">{label}</p><p className="text-2xl font-bold text-white">{value}</p><p className="text-xs text-slate-500">{helper}</p></div>
      </CardContent>
    </Card>
  );
}

function QueueStat({ label, value }: { label: string; value: string | number }) {
  return <div className="rounded-xl bg-slate-950/55 p-3"><p className="text-xs text-slate-400">{label}</p><p className="font-semibold text-white">{value}</p></div>;
}
