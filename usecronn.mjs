import { schedule } from 'node-cron';
import { MongoClient } from 'mongodb';
import 'dotenv/config';

const providers = ["belo", "binance", "bitmonedero", "buenbit", "lemoncash", "ripio"];
const urls = [];
providers.forEach(e => {
    let url = `http://criptoya.com/api/${e}/usdt/ars`
    urls.push(url);
});

const fetchData = async (url) => {
    try {
      const response = await fetch(url);
      const data = await response.json();
      return data;
    } catch (error) {
      console.log(error);
    }
  };

const fetchAllData = async () => {
    try {
        const results = await Promise.all(urls.map(url => fetchData(url)));
        // Extract the broker name from the URL and add it to each response object
        const updatedResults = results.map((response, index) => {
            const urlParts = urls[index].split('/');
            const broker = urlParts[urlParts.length - 3];
            return { ...response, broker };
        });

        // I unify the timestamp to simplify things later.
        const firstTimestamp = updatedResults.length > 0 ? updatedResults[0].time : 0;
        const finalResults = updatedResults.map(response => ({ ...response, time: firstTimestamp }));

      return finalResults;
      
    } catch (error) {
      console.error('Error fetching data:', error);
    }
};

async function fetchDataAndSaveToMongo() {
  const db = await connectToDatabase();
  const crypto_data = await fetchAllData()
  const collectionName = 'conversion_rate';
  const result = await db.collection(collectionName).insertMany(crypto_data);
  console.log(`Inserted ${result.insertedCount} documents into the ${collectionName} collection`);

  console.log('Task completed successfully!');
}

// Schedule the task to run every hour
schedule('0 */2 * * *', async () => {
  console.log('Running the task...');
  await fetchDataAndSaveToMongo();
});

console.log('Scheduler started.');

const uri = process.env.MONGODB_URI_NEW;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

export async function connectToDatabase() {
  try {
    await client.connect();
    return client.db('usdt');
  } catch (error) {
    console.error('Error connecting to MongoDB:', error);
    throw error;
  }
}