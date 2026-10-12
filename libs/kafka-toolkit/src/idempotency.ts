import { ClientSession, Model } from 'mongoose';
import type { ProcessedEventDocument } from './schemas/processed-event.schema';

export type IdempotencyResult = 'processed' | 'duplicate';

/**
 * Helper de idempotencia para consumidores (RNF-03).
 *
 * Inserta el eventId en `processed_events` y ejecuta el efecto del consumidor
 * dentro de la MISMA transacción de Mongo: o quedan ambos o no queda nada.
 *
 * Si otra entrega del mismo evento ya registró el eventId, la inserción falla
 * con duplicate-key (E11000 don índice único) y el evento se descarta sin
 * ejecutar el efecto: la operación es exactamente-una-vez sobre la escritura,
 * incluso ante reentregas de Kafka (at-least-once delivery).
 *
 * El resultado del efecto se puede guardar en `boxed` para publicar/emitir
 * DESPUÉS del commit (los efectos externos no pueden revertirse con la tx).
 */
export async function consumeIdempotently<T = void>(
  processedEventModel: Model<ProcessedEventDocument>,
  eventId: string,
  effect: (session: ClientSession) => Promise<T>,
): Promise<{ result: IdempotencyResult; value?: T }> {
  if (!eventId) {
    // Sin eventId no hay forma de deduplicar: se descarta para no aplicar
    // dos veces un mensaje potencialmente repetido.
    return { result: 'duplicate' };
  }

  const session = await processedEventModel.db.startSession();
  try {
    let value: T | undefined;
    await session.withTransaction(async () => {
      await processedEventModel.create(
        [{ _id: eventId, eventId, processedAt: new Date() }],
        { session },
      );
      value = await effect(session);
    });
    return { result: 'processed', value };
  } catch (error: unknown) {
    const err = error as { code?: number };
    if (err?.code === 11000) {
      // Ya procesado por otra entrega: descartar sin efecto.
      return { result: 'duplicate' };
    }
    throw error;
  } finally {
    await session.endSession();
  }
}
