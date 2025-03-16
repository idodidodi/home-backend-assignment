import { StreetsService } from "../israelistreets/StreetsService";
import { KafkaService } from "../kafka/KafkaService";

async function main() {
    const args = process.argv.slice(2);
    
    if (args.includes('--help')) {
        console.log('Usage: npm run dev src/publish-service/index.ts -- "<city name>"');
        console.log('Note: City name must be in English');
        console.log('Examples:');
        console.log('  npm run dev src/publish-service/index.ts -- "Tel Aviv Jaffa"');
        console.log('  npm run dev src/publish-service/index.ts -- "Jerusalem"');
        process.exit(0);
    }

    try {
        // Join all arguments to handle multi-word city names
        const cityName = args.join(' ');
        console.log('Fetching streets for city:', cityName);
        
        // Get streets data
        const result = await StreetsService.getStreetsInCity(cityName);
        console.log(`Found ${result.streets.length} streets in ${result.city}`);
        
        // Publish to Kafka
        console.log('Publishing streets data to Kafka...');
        await KafkaService.publishStreets(result.city, result.streets);
        
        // Cleanup
        await KafkaService.disconnect();
    } catch (error) {
        console.error('Error:', error.message);
        // Ensure we disconnect from Kafka even if there's an error
        await KafkaService.disconnect();
        process.exit(1);
    }
}

// Handle process termination
process.on('SIGTERM', async () => {
    console.log('Received SIGTERM. Cleaning up...');
    await KafkaService.disconnect();
    process.exit(0);
});

process.on('SIGINT', async () => {
    console.log('Received SIGINT. Cleaning up...');
    await KafkaService.disconnect();
    process.exit(0);
});

main().catch(console.error);