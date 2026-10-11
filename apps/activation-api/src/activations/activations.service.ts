import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { Activation, ActivationDocument, ActivationStatus, HistoryEntry } from './activation.schema';
import { CreateActivationDto } from './dto/activation.dto';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

@Injectable()
export class ActivationsService {
  constructor(
    @InjectModel(Activation.name) private readonly activationModel: Model<ActivationDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async create(dto: CreateActivationDto): Promise<{ activationId: string; status: ActivationStatus }> {
    const activationId = `act-${uuidv4().slice(0, 8)}`;
    const now = new Date().toISOString();

    const history: HistoryEntry[] = [
      {
        eventType: 'ActivationRequested',
        at: now,
        details: { planId: dto.planId, channel: 'web' },
      },
    ];

    const activation = new this.activationModel({
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      status: 'PENDING',
      simulateFailure: dto.simulateFailure ?? 'none',
      steps: {
        billing: { status: 'PENDING' },
        provisioning: { status: 'PENDING' },
      },
      history,
    });

    await activation.save();

    await this.kafkaProducer.publishActivationRequested(
      activationId,
      dto.customerId,
      dto.planId,
      dto.simulateFailure ?? 'none',
    );

    return { activationId, status: 'PENDING' };
  }

  async findById(id: string): Promise<ActivationDetailsResponseDto> {
    const activation = await this.activationModel.findById(id).lean().exec();
    if (!activation) {
      throw new NotFoundException(`Activation with id ${id} not found`);
    }
    return activation as unknown as ActivationDetailsResponseDto;
  }
}

interface ActivationDetailsResponseDto {
  _id: string;
  customerId: string;
  planId: string;
  status: ActivationStatus;
  simulateFailure: string;
  steps: {
    billing: { status: string; at?: string; reason?: string };
    provisioning: { status: string; at?: string; reason?: string };
  };
  history: Array<{ eventType: string; at: string; details?: Record<string, unknown> }>;
  createdAt: Date;
  updatedAt: Date;
}