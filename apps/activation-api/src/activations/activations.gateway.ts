import {
  ConnectedSocket,
  MessageBody,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';

export const ALL_ACTIVATIONS_ROOM = 'all';

export interface ActivationEventMessage {
  activationId: string;
  eventType: string;
  at: string;
  status: string;
}

@WebSocketGateway({
  cors: {
    // localhost:5173 = demo-ui en desarrollo. 'null' cubre páginas locales abiertas
    // con file:// (como test/ws-test.html), que no envían Origin.
    origin: ['http://localhost:5173', 'null'],
  },
})
export class ActivationsGateway {
  @WebSocketServer()
  server!: Server;

  /**
   * Los clientes se suscriben a una activación concreta (room = activationId)
   * o a todas (room = 'all').
   */
  @SubscribeMessage('subscribe')
  handleSubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { activationId?: string },
  ): { subscribed: string } {
    const room = body?.activationId ?? ALL_ACTIVATIONS_ROOM;
    client.join(room);
    return { subscribed: room };
  }

  emitActivationEvent(message: ActivationEventMessage): void {
    // Broadcast a la room de la activación y a la room global. socket.io deduplica:
    // un cliente suscripto a ambas recibe una sola copia del evento.
    this.server
      .to(message.activationId)
      .to(ALL_ACTIVATIONS_ROOM)
      .emit('activation-event', message);
  }
}
