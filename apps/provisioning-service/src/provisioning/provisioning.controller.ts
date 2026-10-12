import { Controller } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ProvisioningService } from './provisioning.service';
import { TOPICS } from '@activation-poc/contracts';
import {
  assertEventEnvelope,
  DlqHandler,
  toRawKafkaMessage,
} from '@activation-poc/kafka-toolkit';
import type { ActivationRequestedEvent } from '@activation-poc/contracts';

/**
 * Controlador-consumidor Kafka de provisioning-service: escucha el topic
 * activation.requested y delega en ProvisioningService con protección DLQ.
 */
@Controller()
export class ProvisioningController {
  private readonly dlq: DlqHandler;

  constructor(
    private readonly provisioningService: ProvisioningService,
    config: ConfigService,
  ) {
    this.dlq = new DlqHandler(
      config.getOrThrow<string[]>('kafka.brokers'),
      'provisioning-service-dlq',
    );
  }

  /**
   * Escucha el topic activation.requested y procesa el evento con DLQ.
   * @param event Evento ActivationRequested.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: ActivationRequestedEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.dlq.run(
      toRawKafkaMessage({ topic: context.getTopic(), ...context.getMessage() }),
      async () => {
        assertEventEnvelope(event);
        await this.provisioningService.handleActivationRequested(event);
      },
    );
  }
}
