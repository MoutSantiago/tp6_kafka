import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { appConfig, kafkaConfig } from './config/env.config';
import { LoyaltyModule } from './loyalty/loyalty.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [appConfig, kafkaConfig],
    }),
    LoyaltyModule,
  ],
})
export class AppModule {}
