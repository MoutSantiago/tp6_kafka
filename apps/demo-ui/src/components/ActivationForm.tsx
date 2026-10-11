interface Props {
  customerId: string;
  planId: string;
  simulateFailure: 'none' | 'billing' | 'provisioning';
  sending: boolean;
  onCustomerIdChange: (v: string) => void;
  onPlanIdChange: (v: string) => void;
  onSimulateFailureChange: (v: 'none' | 'billing' | 'provisioning') => void;
  onSubmit: () => void;
}

export function ActivationForm(props: Props) {
  return (
    <form
      className="panel form"
      onSubmit={(e) => {
        e.preventDefault();
        props.onSubmit();
      }}
    >
      <label>
        Cliente
        <input
          value={props.customerId}
          onChange={(e) => props.onCustomerIdChange(e.target.value)}
          placeholder="C-1234"
          required
        />
      </label>

      <label>
        Plan
        <input
          value={props.planId}
          onChange={(e) => props.onPlanIdChange(e.target.value)}
          placeholder="PLAN DIGITAL"
          required
        />
      </label>

      <label>
        Forzar fallo
        <select
          value={props.simulateFailure}
          onChange={(e) =>
            props.onSimulateFailureChange(e.target.value as 'none' | 'billing' | 'provisioning')
          }
        >
          <option value="none">Ninguno</option>
          <option value="billing">Billing</option>
          <option value="provisioning">Provisioning</option>
        </select>
      </label>

      <button type="submit" disabled={props.sending}>
        {props.sending ? 'Contratando…' : 'Contratar plan'}
      </button>
    </form>
  );
}
