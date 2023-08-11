import { Queue, Worker } from 'bullmq';
import 'dotenv/config';
import express, { json } from 'express';
import { Pool } from 'pg';

// create a db connection
const connectionString = process.env.DATABASE_URL;
const pool = new Pool({
  connectionString,
});

// redis connection config
const connection = {
  host: process.env.REDIS_HOST,
  port: parseInt(process.env.REDIS_PORT ?? '6379'),
};

// Create a job queue
const queue = new Queue('etl', {
  connection,
});

// Create a job worker
const worker = new Worker(
  'etl',
  async (job) => {
    if (job.name === 'etl') {
      console.log('doing etl');
      // do the ETL
      // create the tables if they don't exist
      try {
        await pool.query('BEGIN');
        await pool.query(`
          CREATE TABLE IF NOT EXISTS contacts (
            id text,
            first_name text,
            last_name text,
            email text,
            phone text,
            created_at timestamp,
            updated_at timestamp,
            customer_id text,
            PRIMARY KEY (id, customer_id)
          );
        `);
        await pool.query(`
          CREATE TABLE IF NOT EXISTS opportunities (
            id text PRIMARY KEY,
            name text,
            amount numeric,
            stage text,
            created_at timestamp,
            updated_at timestamp,
            customer_id text,
            PRIMARY KEY (id, customer_id)
          );
        `);
        await pool.query(`
          CREATE TABLE IF NOT EXISTS accounts (
            id text,
            name text,
            created_at timestamp,
            updated_at timestamp,
            customer_id text,
            PRIMARY KEY (id, customer_id)
          );
        `);
        await pool.query(`
          CREATE TABLE IF NOT EXISTS entity_accounts (
            entity_id text,
            account_id text,
            entity_type text,
            _supaglue_customer_id text,
            _supaglue_application_id, text,
            PRIMARY KEY (entity_id, account_id, entity_type, _supaglue_customer_id, _supaglue_application_id)
          );`);
        await pool.query(`
            INSERT INTO contacts (id, first_name, last_name, email, phone, created_at, updated_at, customer_id)
            SELECT id, first_name, last_name, email, phone, created_at, updated_at
            FROM crm_accounts (id, first_name, last_name, email_addresses->0->'email_address', phone_numbers->0->phone_number, created_at, updated_at, _supaglue_customer_id)
            ON CONFLICT (id, customer_id) DO UPDATE SET
              first_name = EXCLUDED.first_name,
              last_name = EXCLUDED.last_name,
              email = EXCLUDED.email,
              phone = EXCLUDED.phone,
              created_at = EXCLUDED.created_at,
              updated_at = EXCLUDED.updated_at;
        `);
        await pool.query(`
            INSERT INTO opportunities (id, name, amount, stage, created_at, updated_at, customer_id)
            SELECT id, name, amount, stage, created_at, updated_at, _supaglue_customer_id
            FROM crm_opportunities (id, name, amount, stage, created_at, updated_at, _supaglue_customer_id)
            ON CONFLICT (id, customer_id) DO UPDATE SET
              name = EXCLUDED.name,
              amount = EXCLUDED.amount,
              stage = EXCLUDED.stage,
              created_at = EXCLUDED.created_at,
              updated_at = EXCLUDED.updated_at;
        `);
        await pool.query(`
            INSERT INTO accounts (id, name, created_at, updated_at, customer_id)
            SELECT id, name, created_at, updated_at, _supaglue_customer_id
            FROM crm_accounts (id, name, created_at, updated_at, _supaglue_customer_id)
            ON CONFLICT (id, customer_id) DO UPDATE SET
              name = EXCLUDED.name,
              created_at = EXCLUDED.created_at,
              updated_at = EXCLUDED.updated_at;
        `);
        await pool.query(`
            INSERT INTO entity_accounts (entity_id, account_id, entity_type, _supaglue_customer_id, _supaglue_application_id)
            SELECT id, account_id, 'contact', _supaglue_customer_id, _supaglue_application_id
            FROM crm_accounts (id, account_id, _supaglue_customer_id, _supaglue_application_id)
            ON CONFLICT (entity_id, account_id, entity_type, _supaglue_customer_id, _supaglue_application_id) DO NOTHING;
        `);
        await pool.query(`
            INSERT INTO entity_accounts (entity_id, account_id, entity_type, _supaglue_customer_id, _supaglue_application_id)
            SELECT id, account_id, 'opportunity', _supaglue_customer_id, _supaglue_application_id
            FROM crm_opportunities (id, account_id, _supaglue_customer_id, _supaglue_application_id)
            ON CONFLICT (entity_id, account_id, entity_type, _supaglue_customer_id, _supaglue_application_id) DO NOTHING;
        `);
        await pool.query('COMMIT');
      } catch (err) {
        await pool.query('ROLLBACK');
        throw err;
      }
    }
  },
  {
    connection,
  }
);

const app = express();

// Define middleware
app.use(json());

// function to check if a set is a superset of another set
function isSuperset(set: Set<unknown>, subset: Set<unknown>) {
  for (const elem of subset) {
    if (!set.has(elem)) {
      return false;
    }
  }
  return true;
}

// keep track of the object events we have recieved so we can run the ETL when we have all of them
// TODO move this to redis
const collectedObjectEvents: Record<string, Set<string>> = {};
const desiredObjectEvents = new Set(['contact', 'opportunity', 'account']);

async function maybeDoETL(event: any) {
  // skip recording the event if it's not a success
  if (event.result !== 'SUCCESS') {
    return;
  }

  const collectedObjectEventsForConnectionId = collectedObjectEvents[event.connection_id] ?? new Set();
  collectedObjectEventsForConnectionId.add(event.object);
  collectedObjectEvents[event.connection_id] = collectedObjectEventsForConnectionId;

  // only do the ETL if we have all the events we want
  if (isSuperset(collectedObjectEventsForConnectionId, desiredObjectEvents)) {
    await queue.add('etl', {});
    collectedObjectEventsForConnectionId.clear();
  }
}

// Define routes
app.post('/webhook', async (req, res, next) => {
  try {
    console.log(req.body);
    await maybeDoETL(req.body);
    res.send({ status: 'OK' });
  } catch (err) {
    console.error(err);
    next(err);
  }
});

// Start the server
const PORT = process.env.PORT ?? 3000;
const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

server.on('close', async () => {
  await pool.end();
});
