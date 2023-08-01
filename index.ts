import 'dotenv/config';
import express, { json } from 'express';

const app = express();

// Define middleware
app.use(json());

// Define routes
app.get('/', (req, res) => {
  res.send('Hello World!');
});

// Start the server
const PORT = process.env.PORT ?? 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
