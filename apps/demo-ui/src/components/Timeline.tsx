import type { TimelineEvent } from '../App';

const SUCCESS_EVENTS = new Set([
  'ActivationRequested',
  'BillingAccountCreated',
  'ProvisioningCompleted',
  'ActivationCompleted',
]);

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString('es-AR', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

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
