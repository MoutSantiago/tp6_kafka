import { Controller, Get, Post, Body, Param, HttpCode, HttpStatus } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from '@nestjs/swagger';
import { ActivationsService } from './activations.service';
import { CreateActivationDto, ActivationResponseDto, ActivationDetailsResponseDto } from './dto/activation.dto';

@ApiTags('activations')
@Controller('activations')
export class ActivationsController {
  constructor(private readonly activationsService: ActivationsService) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({ summary: 'Create a new activation' })
  @ApiResponse({ status: 202, description: 'Activation created', type: ActivationResponseDto })
  @ApiResponse({ status: 400, description: 'Bad request' })
  async create(@Body() dto: CreateActivationDto): Promise<ActivationResponseDto> {
    return this.activationsService.create(dto);
  }

  @Get(':id')
  @ApiOperation({ summary: 'Get activation details by ID' })
  @ApiParam({ name: 'id', example: 'act-0001' })
  @ApiResponse({ status: 200, description: 'Activation details', type: ActivationDetailsResponseDto })
  @ApiResponse({ status: 404, description: 'Activation not found' })
  async findById(@Param('id') id: string): Promise<ActivationDetailsResponseDto> {
    return this.activationsService.findById(id);
  }
}