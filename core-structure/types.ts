export interface MessageEnvelope<T> {
  eventId: string;
  eventType: string;
  version: string;
  payload: T;
  metadata?: {
    correlationId?: string;
    source?: string;
    timestamp?: string;
  };
}