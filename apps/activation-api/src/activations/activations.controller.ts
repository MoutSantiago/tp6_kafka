import { Controller, Get, Post, Body, Param, HttpCode, HttpStatus } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from '@nestjs/swagger';
import { ActivationsService } from './activations.service';
import { CreateActivationDto, ActivationResponseDto, ActivationDetailsResponseDto } from './dto/activation.dto';

/**
 * Controlador REST de activaciones: permite crear una activación
 * (POST /activations, responde 202) y consultar su detalle (GET /activations/:id).
 */
@ApiTags('activations')
@Controller('activations')
export class ActivationsController {
  constructor(private readonly activationsService: ActivationsService) {}

  /**
   * Crea una nueva activación y publica ActivationRequested en Kafka.
   * @param dto Datos de la solicitud (customerId, planId, simulateFailure opcional).
   * @returns ID de la activación y estado inicial.
   */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({ summary: 'Create a new activation' })
  @ApiResponse({ status: 202, description: 'Activation created', type: ActivationResponseDto })
  @ApiResponse({ status: 400, description: 'Bad request' })
  async create(@Body() dto: CreateActivationDto): Promise<ActivationResponseDto> {
    return this.activationsService.create(dto);
  }

  /**
   * Devuelve el detalle completo de una activación (estado, pasos e historial).
   * @param id ID de la activación (ej. act-0001).
   * @returns Detalle de la activación o 404 si no existe.
   */
  @Get(':id')
  @ApiOperation({ summary: 'Get activation details by ID' })
  @ApiParam({ name: 'id', example: 'act-0001' })
  @ApiResponse({ status: 200, description: 'Activation details', type: ActivationDetailsResponseDto })
  @ApiResponse({ status: 404, description: 'Activation not found' })
  async findById(@Param('id') id: string): Promise<ActivationDetailsResponseDto> {
    return this.activationsService.findById(id);
  }
}