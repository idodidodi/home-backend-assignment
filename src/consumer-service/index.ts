import { Kafka, Consumer } from 'kafkajs';
import { Client } from 'pg';
import { StreetsMessage } from '../kafka/KafkaService';

class ConsumerService {
    private static _consumer: Consumer;
    private static _pgClient: Client;
    private static readonly TOPIC = 'streets-data';
    private static readonly CLIENT_ID = 'streets-consumer';
    private static readonly GROUP_ID = 'streets-consumer-group';

    private static async getConsumer(): Promise<Consumer> {
        if (!this._consumer) {
            const kafka = new Kafka({
                clientId: this.CLIENT_ID,
                brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
            });
            this._consumer = kafka.consumer({ groupId: this.GROUP_ID });
            await this._consumer.connect();
        }
        return this._consumer;
    }

    private static async getPgClient(): Promise<Client> {
        if (!this._pgClient) {
            this._pgClient = new Client({
                host: process.env.POSTGRES_HOST || 'localhost',
                port: parseInt(process.env.POSTGRES_PORT || '5432'),
                database: process.env.POSTGRES_DB || 'streets',
                user: process.env.POSTGRES_USER || 'postgres',
                password: process.env.POSTGRES_PASSWORD || 'postgres',
            });
            await this._pgClient.connect();

            // Create table if not exists
            await this._pgClient.query(`
                CREATE TABLE IF NOT EXISTS streets (    
                    street_id INT PRIMARY KEY,
                    street_name TEXT NOT NULL,
                    region_code NUMERIC NOT NULL,
                    region_name TEXT NOT NULL,
                    city_code NUMERIC NOT NULL,
                    city_name TEXT NOT NULL,
                    street_code NUMERIC NOT NULL,
                    street_name_status TEXT NOT NULL,
                    official_code NUMERIC NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            `);
        }
        return this._pgClient;
    }

    private static async handleMessage(message: any) {
        try {
            const data: StreetsMessage = JSON.parse(message.value.toString());
            const pgClient = await this.getPgClient();

            // Begin transaction
            await pgClient.query('BEGIN');

            try {
                // Insert each street
                for (const street of data.streets) {
                    await pgClient.query(
                        `INSERT INTO streets (
                            street_id, street_name, region_code, region_name,
                            city_code, city_name, street_code,
                            street_name_status, official_code,
                            created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (street_id) DO UPDATE SET
                            street_name = $2,
                            region_code = $3,
                            region_name = $4,
                            city_code = $5,
                            city_name = $6,
                            street_code = $7,
                            street_name_status = $8,
                            official_code = $9,
                            updated_at = CURRENT_TIMESTAMP`,
                        [
                            street.streetId,
                            street.street_name,
                            street.region_code,
                            street.region_name,
                            street.city_code,
                            data.city,
                            street.street_code,
                            street.street_name_status,
                            street.official_code
                        ]
                    );
                }

                // Commit transaction
                await pgClient.query('COMMIT');
                console.log(`Successfully stored ${data.streets.length} streets for ${data.city}`);
            } catch (error) {
                // Rollback transaction on error
                await pgClient.query('ROLLBACK');
                throw error;
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    }

    static async start() {
        try {
            const consumer = await this.getConsumer();

            // Subscribe to the topic
            await consumer.subscribe({ topic: this.TOPIC, fromBeginning: true });

            // Start processing messages
            await consumer.run({
                eachMessage: async ({ message }) => {
                    await this.handleMessage(message);
                },
            });

            console.log(`Consumer started. Listening to topic: ${this.TOPIC}`);

            // Handle graceful shutdown
            process.on('SIGTERM', async () => {
                console.log('Received SIGTERM. Cleaning up...');
                await this.shutdown();
                process.exit(0);
            });

            process.on('SIGINT', async () => {
                console.log('Received SIGINT. Cleaning up...');
                await this.shutdown();
                process.exit(0);
            });

        } catch (error) {
            console.error('Failed to start consumer:', error);
            await this.shutdown();
            process.exit(1);
        }
    }

    static async shutdown() {
        if (this._consumer) {
            await this._consumer.disconnect();
        }
        if (this._pgClient) {
            await this._pgClient.end();
        }
    }
}

// Start the consumer service
ConsumerService.start().catch(console.error); 