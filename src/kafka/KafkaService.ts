import { Kafka, Producer } from 'kafkajs';
import { city } from '../israelistreets/cities';
import { Street } from '../israelistreets/StreetsService';

export interface StreetsMessage {
    city: city;
    timestamp: string;
    streets: Pick<Street, 'streetId' | 'street_name'>[];
}

export class KafkaService {
    private static _producer: Producer;
    private static readonly TOPIC = 'streets-data';
    private static readonly CLIENT_ID = 'streets-publisher';

    private static async getProducer(): Promise<Producer> {
        if (!this._producer) {
            const kafka = new Kafka({
                clientId: this.CLIENT_ID,
                brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
            });
            this._producer = kafka.producer();
            await this._producer.connect();
        }
        return this._producer;
    }

    static async publishStreets(city: city, streets: Pick<Street, 'streetId' | 'street_name'>[]): Promise<void> {
        const producer = await this.getProducer();
        
        const message: StreetsMessage = {
            city,
            timestamp: new Date().toISOString(),
            streets
        };

        try {
            await producer.send({
                topic: this.TOPIC,
                messages: [
                    { 
                        key: city,
                        value: JSON.stringify(message)
                    }
                ]
            });
            console.log(`Successfully published ${streets.length} streets for ${city} to Kafka`);
        } catch (error) {
            console.error('Failed to publish to Kafka:', error);
            throw new Error(`Failed to publish streets data for ${city} to Kafka: ${error.message}`);
        }
    }

    static async disconnect(): Promise<void> {
        if (this._producer) {
            await this._producer.disconnect();
        }
    }
} 