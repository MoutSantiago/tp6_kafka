const CLASS_BY_STATUS: Record<string, string> = {
  PENDING: 'badge pending',
  IN_PROGRESS: 'badge progress',
  ACTIVE: 'badge ok',
  FAILED: 'badge error',
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <span className={CLASS_BY_STATUS[status] ?? 'badge'}>{status}</span>
  );
}
