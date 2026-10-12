import { Module } from '@nestjs/common';
import { LoyaltyController } from './loyalty.controller';

@Module({
  controllers: [LoyaltyController],
})
/** Módulo de loyalty: registra el consumidor Kafka que loguea todos los eventos. */
export class LoyaltyModule {}
