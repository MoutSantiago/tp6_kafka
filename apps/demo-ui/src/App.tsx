import { useCallback, useEffect, useRef, useState } from 'react';
import { io, Socket } from 'socket.io-client';
import { ActivationForm } from './components/ActivationForm';
import { StatusBadge } from './components/StatusBadge';
import { Timeline } from './components/Timeline';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';

/** Evento de activación recibido por WebSocket para mostrar en la timeline. */
export interface TimelineEvent {
  activationId: string;
  eventType: string;
  at: string;
  status: string;
}

/** Detalle de una activación tal como lo devuelve la API REST de activation-api. */
export interface ActivationDetails {
  _id: string;
  customerId: string;
  planId: string;
  status: string;
  steps: {
    billing: { status: string; at?: string; reason?: string };
    provisioning: { status: string; at?: string; reason?: string };
  };
  history: Array<{ eventType: string; at: string; details?: Record<string, unknown> }>;
}

/** Etapa en la que se simula un fallo (o 'none'). */
type SimulateFailure = 'none' | 'billing' | 'provisioning';

/**
 * Componente raíz de la demo UI: formulario para iniciar una activación,
 * suscripción WebSocket a sus eventos y visualización del estado de la saga
 * y la timeline de eventos en tiempo real.
 */
export function App() {
  const [customerId, setCustomerId] = useState('C-demo');
  const [planId, setPlanId] = useState('PLAN DEMO');
  const [simulateFailure, setSimulateFailure] = useState<SimulateFailure>('none');
  const [activation, setActivation] = useState<ActivationDetails | null>(null);
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const socketRef = useRef<Socket | null>(null);
  const lastIdRef = useRef<string | null>(null);

  useEffect(() => {
    const socket = io(API_URL);
    socketRef.current = socket;

    socket.on('connect', () => {
      console.log('WS conectado:', socket.id);
      // Si ya había una activación seleccionada (reconexión), volver a suscribirse.
      if (lastIdRef.current) {
        socket.emit('subscribe', { activationId: lastIdRef.current });
      }
    });
    socket.on('connect_error', (err) => setError(`Error de conexión WS: ${err.message}`));
    socket.on('activation-event', (msg: TimelineEvent) => {
      // El servidor ya filtra por room, pero validamos por las dudas.
      if (msg.activationId !== lastIdRef.current) return;
      setEvents((prev) =>
        prev.some((e) => e.eventType === msg.eventType && e.at === msg.at) ? prev : [...prev, msg],
      );
      setActivation((prev) => (prev ? { ...prev, status: msg.status } : prev));
    });

    return () => {
      socket.disconnect();
      socketRef.current = null;
    };
  }, []);

  /**
   * Consulta el detalle de una activación en la API REST y siembra la timeline
   * con el historial persistido (deduplicando por eventType + fecha).
   * @param id ID de la activación a consultar.
   */
  const fetchActivation = useCallback(async (id: string) => {
    try {
      const res = await fetch(`${API_URL}/activations/${id}`);
      if (!res.ok) return;
      const data = (await res.json()) as ActivationDetails;
      setActivation(data);
      // Sembrar la timeline con el history persistido (por si el primer evento
      // se emitió antes de completar la suscripción WS).
      // El dedupe por (eventType, at) evita duplicados con los próximos mensajes WS.
      const seeded: TimelineEvent[] = data.history.map((h) => ({
        activationId: data._id,
        eventType: h.eventType,
        at: h.at,
        status: data.status,
      }));
      setEvents((prev) => {
        const merged = [...prev];
        for (const s of seeded) {
          if (!merged.some((e) => e.eventType === s.eventType && e.at === s.at)) merged.push(s);
        }
        return merged;
      });
    } catch (err) {
      console.error(err);
    }
  }, []);

  /**
   * Crea una activación vía POST a la API, resetea la vista, suscribe el
   * WebSocket a la nueva activación y sincroniza el estado inicial.
   */
  const handleSubmit = async () => {
    setSending(true);
    setError(null);
    try {
      const res = await fetch(`${API_URL}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, planId, simulateFailure }),
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const data = (await res.json()) as { activationId: string; status: string };
      lastIdRef.current = data.activationId;
      setEvents([]);
      setActivation({
        _id: data.activationId,
        customerId,
        planId,
        status: data.status,
        steps: {
          billing: { status: 'PENDING' },
          provisioning: { status: 'PENDING' },
        },
        history: [],
      });
      // Suscripción WebSocket a la activación recién creada.
      socketRef.current?.emit('subscribe', { activationId: data.activationId });
      // Sincronizar al estado real por si llegó algún evento antes de suscribirnos.
      await fetchActivation(data.activationId);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error desconocido');
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="container">
      <header>
        <h1>POC Activación de Servicios</h1>
        <p className="subtitle">
          Contratá un plan y seguí los eventos de la saga en tiempo real
        </p>
      </header>

      <ActivationForm
        customerId={customerId}
        planId={planId}
        simulateFailure={simulateFailure}
        sending={sending}
        onCustomerIdChange={setCustomerId}
        onPlanIdChange={setPlanId}
        onSimulateFailureChange={setSimulateFailure}
        onSubmit={handleSubmit}
      />

      {error && <div className="alert">{error}</div>}

      {activation && (
        <section className="panel">
          <div className="activation-header">
            <h2>{activation._id}</h2>
            <StatusBadge status={activation.status} />
          </div>
          <div className="steps">
            <span>
              Billing: <strong>{activation.steps.billing.status}</strong>
            </span>
            <span>
              Provisioning: <strong>{activation.steps.provisioning.status}</strong>
            </span>
          </div>
          <Timeline events={events} />
        </section>
      )}

      {!activation && (
        <p className="hint">
          Elegí un cliente, un plan y (opcionalmente) un fallo a simular. Al
          enviar el formulario te suscribís por WebSocket a esa activación y
          cada evento aparece acá en vivo: verde si es éxito, rojo si es fallo
          o compensación.
        </p>
      )}
    </div>
  );
}
