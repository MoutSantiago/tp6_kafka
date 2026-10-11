import { Controller, Get, HttpCode, HttpStatus, Query } from '@nestjs/common';
import { EventsService } from './events.service';
import type { EventLogDocument } from './event-log.schema';

@Controller('events')
export class EventsController {
  constructor(private readonly eventsService: EventsService) {}

  @Get()
  @HttpCode(HttpStatus.OK)
  async findByCorrelationId(
    @Query('correlationId') correlationId: string,
  ): Promise<EventLogDocument[]> {
    return this.eventsService.findByCorrelationId(correlationId);
  }
}
