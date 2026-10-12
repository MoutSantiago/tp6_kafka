import type { TimelineEvent } from '../App';

/** Tipos de evento considerados de éxito (se muestran en verde); el resto se muestra en rojo. */
const SUCCESS_EVENTS = new Set([
  'ActivationRequested',
  'BillingAccountCreated',
  'ProvisioningCompleted',
  'ActivationCompleted',
]);

/**
 * Formatea una fecha ISO como hora local hh:mm:ss.
 * @param iso Fecha en formato ISO.
 * @returns Hora formateada.
 */
function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString('es-AR', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

/**
 * Lista visual de la timeline de eventos de la activación: verde para eventos
 * de éxito, rojo para fallos y compensaciones.
 * @param events Eventos recibidos hasta el momento.
 */
export function Timeline({ events }: { events: TimelineEvent[] }) {
  if (events.length === 0) {
    return <p className="hint">Esperando eventos…</p>;
  }

  return (
    <ul className="timeline">
      {events.map((e, i) => {
        const ok = SUCCESS_EVENTS.has(e.eventType);
        return (
          <li key={`${e.eventType}-${e.at}-${i}`} className={ok ? 'ok' : 'fail'}>
            <span className="dot" />
            <span className="time">{formatTime(e.at)}</span>
            <span className="event-type">{e.eventType}</span>
            <span className="status">{e.status}</span>
          </li>
        );
      })}
    </ul>
  );
}
