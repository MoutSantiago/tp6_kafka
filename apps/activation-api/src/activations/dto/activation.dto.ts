import { IsString, IsOptional, IsIn } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/** DTO de entrada para crear una activación (POST /activations). */
export class CreateActivationDto {
  @ApiProperty({ example: 'C-1234' })
  @IsString()
  customerId!: string;

  @ApiProperty({ example: 'PLAN_DIGITAL' })
  @IsString()
  planId!: string;

  @ApiPropertyOptional({ enum: ['none', 'billing', 'provisioning'], default: 'none' })
  @IsOptional()
  @IsIn(['none', 'billing', 'provisioning'])
  simulateFailure?: 'none' | 'billing' | 'provisioning';
}

/** DTO de respuesta al crear una activación (ID y estado inicial). */
export class ActivationResponseDto {
  @ApiProperty({ example: 'act-0001' })
  activationId!: string;

  @ApiProperty({ enum: ['PENDING', 'IN_PROGRESS', 'ACTIVE', 'FAILED'] })
  status!: string;
}

/** DTO de respuesta con el detalle completo de una activación (estado, pasos e historial). */
export class ActivationDetailsResponseDto {
  @ApiProperty()
  _id!: string;

  @ApiProperty()
  customerId!: string;

  @ApiProperty()
  planId!: string;

  @ApiProperty({ enum: ['PENDING', 'IN_PROGRESS', 'ACTIVE', 'FAILED'] })
  status!: string;

  @ApiProperty({ enum: ['none', 'billing', 'provisioning'] })
  simulateFailure!: string;

  @ApiProperty()
  steps!: {
    billing: { status: string; at?: string; reason?: string };
    provisioning: { status: string; at?: string; reason?: string };
  };

  @ApiProperty()
  history!: Array<{ eventType: string; at: string; details?: Record<string, unknown> }>;

  @ApiProperty()
  createdAt!: Date;

  @ApiProperty()
  updatedAt!: Date;
}