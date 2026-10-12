import { Controller, Get, HttpCode, HttpStatus, Query } from '@nestjs/common';
import { EventsService } from './events.service';
import type { EventLogDocument } from './event-log.schema';

/**
 * Controlador REST de crm-analytics-service: expone la consulta del log de
 * eventos registrados por correlationId (GET /events?correlationId=...).
 */
@Controller('events')
export class EventsController {
  constructor(private readonly eventsService: EventsService) {}

  /**
   * Devuelve todos los eventos registrados para una activación, en orden temporal.
   * @param correlationId ID de correlación (la activación).
   * @returns Lista de eventos del log.
   */
  @Get()
  @HttpCode(HttpStatus.OK)
  async findByCorrelationId(
    @Query('correlationId') correlationId: string,
  ): Promise<EventLogDocument[]> {
    return this.eventsService.findByCorrelationId(correlationId);
  }
}
