/** Mapeo de estado de la activación a la clase CSS de la insignia. */
const CLASS_BY_STATUS: Record<string, string> = {
  PENDING: 'badge pending',
  IN_PROGRESS: 'badge progress',
  ACTIVE: 'badge ok',
  FAILED: 'badge error',
};

/**
 * Insignia de estado de la activación (PENDING/IN_PROGRESS/ACTIVE/FAILED).
 * @param status Estado actual de la saga.
 */
export function StatusBadge({ status }: { status: string }) {
  return (
    <span className={CLASS_BY_STATUS[status] ?? 'badge'}>{status}</span>
  );
}
